from src.celery_config import celery_app
from src.call_model.resume_analysis_model import evaluate_resume_sync
from src.call_model.resume_structured_model import extract_profile_data
from src.utils.redis_pubsub import publish_message, get_redis_client, increment_batch_progress
import json
import logging
import traceback
import hashlib
import time
# from celery.exceptions import Retry

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def generate_resume_hash(resume_text: str, parameters: dict) -> str:
    """Generate unique hash for resume+parameters combination to prevent duplicates"""
    content = f"{resume_text}||{json.dumps(parameters, sort_keys=True)}"
    return hashlib.md5(content.encode()).hexdigest()


@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def process_resume_task(self, resume_text, parameters, resume_id, batch_id, total_count, candidate_id, round_id):
    """
    Process a single resume with deduplication and comprehensive error handling
    """
    redis_client = get_redis_client()
    status_channel = f"resume_status_{batch_id}"
    # status_channel = f"resume_status"
    # Generate unique hash for deduplication
    resume_hash = generate_resume_hash(resume_text, parameters)
    processing_key = f"processing:{resume_hash}"
    result_key = f"result:{resume_hash}"

    start_time = time.time()

    try:
        cached_result = None  # Initialize variable to avoid scope issues

        # Check if result already exists in cache first
        if redis_client.exists(result_key):
            cached_result = json.loads(redis_client.get(result_key))
            logger.info(f"✅ Found cached result for resume {resume_id}")

            # Atomically increment progress and check completion
            current_count, is_complete = increment_batch_progress(
                batch_id, total_count, redis_client)

            publish_message(status_channel, json.dumps({
                "action": "analysis/result",
                "resume_id": resume_id,
                "candidate_id": candidate_id,
                "round_id": round_id,
                "batch_id": batch_id,
                "resume_result": cached_result,
                "processing_time": time.time() - start_time,
                "cached": True,
                "batch_progress": f"{current_count}/{total_count}"
            }))

            logger.info(f"📤 Results published for resume {resume_id}")
            return {"resume_result": cached_result, "cached": True}

        # Check if this exact resume+parameters is already being processed
        if redis_client.exists(processing_key):
            logger.info(
                f"🔄 Resume {resume_id} already being processed (hash: {resume_hash[:8]})")

            # Wait for the other task to complete and get result
            max_wait = 300  # 5 minutes max wait
            waited = 0
            while redis_client.exists(processing_key) and waited < max_wait:
                time.sleep(2)
                waited += 2

            # Check if result is available after waiting
            if redis_client.exists(result_key):
                cached_result = json.loads(redis_client.get(result_key))
                logger.info(f"✅ Using cached result for resume {resume_id}")

                # Atomically increment progress and check completion
                current_count, is_complete = increment_batch_progress(
                    batch_id, total_count, redis_client)

                # Publish the cached result
                publish_message(status_channel, json.dumps({
                    "action": "analysis/result",
                    "resume_id": resume_id,
                    "batch_id": batch_id,
                    "candidate_id": candidate_id,
                    "round_id": round_id,
                    "resume_result": cached_result,
                    "processing_time": time.time() - start_time,
                    "cached": True,
                    "batch_progress": f"{current_count}/{total_count}"
                }))

                logger.info(f"📤 Results published for resume {resume_id}")
                return {"resume_result": cached_result, "cached": True}
            else:
                # If still no result available, continue with normal processing
                logger.warning(
                    f"⚠️ No cached result found after waiting for resume {resume_id}")

        # Mark as being processed (expires in 10 minutes)
        redis_client.setex(processing_key, 600, "processing")

        logger.info(
            f"🚀 Starting processing for resume {resume_id}/{total_count} (batch: {batch_id})")

        # Publish processing start message
        publish_message(status_channel, json.dumps({
            "action": "analysis/status",
            "message": f"Processing Resume {resume_id}/{total_count}",
            "resume_id": resume_id,
            "batch_id": batch_id,
            "candidate_id": candidate_id,
            "round_id": round_id,
            "progress": 0
        }))

        # Main processing - LLM call
        resume_result = evaluate_resume_sync(resume_text, parameters)

        processing_time = time.time() - start_time

        # Cache the result (expires in 1 hour)
        redis_client.setex(result_key, 3600, json.dumps(resume_result))

        # Remove processing lock
        redis_client.delete(processing_key)

        # Atomically increment progress and check for batch completion
        current_count, is_complete = increment_batch_progress(
            batch_id, total_count, redis_client)

        # Publish success result
        publish_message(status_channel, json.dumps({
            "action": "analysis/result",
            "resume_id": resume_id,
            "batch_id": batch_id,
            "candidate_id": candidate_id,
            "round_id": round_id,
            "resume_result": resume_result,
            "processing_time": processing_time,
            "cached": False,
            "batch_progress": f"{current_count}/{total_count}"
        }))

        logger.info(f"📤 Results published for resume {resume_id}")

        return {
            "resume_result": resume_result,
            "processing_time": processing_time,
            "cached": False,
            "batch_progress": f"{current_count}/{total_count}"
        }

    except Exception as e:
        processing_time = time.time() - start_time

        # Remove processing lock on error
        redis_client.delete(processing_key)

        logger.error(f"❌ Error processing resume {resume_id}: {str(e)}")
        logger.error(f"📋 Error details: {traceback.format_exc()}")

        # Publish error message
        publish_message(status_channel, json.dumps({
            "action": "analysis/error",
            "message": f"Failed to process resume {resume_id}: {str(e)}",
            "resume_id": resume_id,
            "batch_id": batch_id,
            "candidate_id": candidate_id,
            "round_id": round_id,
            "processing_time": processing_time,
            "error_type": type(e).__name__
        }))

        # Retry logic for transient errors
        if self.request.retries < self.max_retries:
            logger.warning(
                f"🔄 Retrying resume {resume_id} (attempt {self.request.retries + 1})")
            raise self.retry(countdown=60 * (self.request.retries + 1))

        # Final failure after all retries
        logger.error(
            f"💀 Final failure for resume {resume_id} after {self.request.retries} retries")
        raise

    finally:
        # Cleanup processing lock if still exists
        if redis_client.exists(processing_key):
            redis_client.delete(processing_key)

        logger.info(f"🏁 Task completed for resume {resume_id}")


@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def struct_resume_task(self, resume_text, resume_id, batch_id, total_count):
    """
    Process a single resume with deduplication and comprehensive error handling

    Args:
        resume_text: The resume content
        parameters: Analysis parameters
        resume_id: Unique identifier for this resume in the batch
        batch_id: Unique identifier for the entire batch
        total_count: Total number of resumes in the batch
    """
    redis_client = get_redis_client()
    status_channel = f"resume_status_{batch_id}"
    # status_channel = f"resume_status"

    # Generate unique hash for deduplication
    resume_hash = generate_resume_hash(resume_text, resume_id)
    processing_key = f"processing:{resume_hash}"
    result_key = f"result:{resume_hash}"

    start_time = time.time()

    try:
        # Check if this exact resume+parameters is already being processed
        if redis_client.exists(processing_key):
            logger.info(
                f"🔄 Resume {resume_id} already being processed (hash: {resume_hash[:8]})")

            # Wait for the other task to complete and get result
            max_wait = 300  # 5 minutes max wait
            waited = 0
            while redis_client.exists(processing_key) and waited < max_wait:
                time.sleep(2)
                waited += 2

            # Check if result is available
            if redis_client.exists(result_key):
                cached_result = json.loads(redis_client.get(result_key))
                logger.info(f"✅ Using cached result for resume {resume_id}")

                # Atomically increment progress and check completion
                current_count, is_complete = increment_batch_progress(
                    batch_id, total_count, redis_client)

                # Publish the cached result
                publish_message(status_channel, json.dumps({
                    "action": "struct/result",
                    "resume_id": resume_id,
                    "batch_id": batch_id,
                    "resume_result": cached_result,
                    "processing_time": time.time() - start_time,
                    "cached": True,
                    "batch_progress": f"{current_count}/{total_count}"
                }))

                return {"resume_result": cached_result, "cached": True}

        # Check if result already exists in cache
        if redis_client.exists(result_key):
            cached_result = json.loads(redis_client.get(result_key))
            logger.info(f"✅ Found cached result for resume {resume_id}")

            # Atomically increment progress and check completion
            current_count, is_complete = increment_batch_progress(
                batch_id, total_count, redis_client)

            # Publish the cached result
            publish_message(status_channel, json.dumps({
                "action": "struct/result",
                "resume_id": resume_id,
                "batch_id": batch_id,
                "resume_result": cached_result,
                "processing_time": time.time() - start_time,
                "cached": True,
                "batch_progress": f"{current_count}/{total_count}"
            }))

            return {"resume_result": cached_result, "cached": True}

        # Mark as being processed (expires in 10 minutes)
        redis_client.setex(processing_key, 600, "processing")

        logger.info(
            f"🚀 Starting processing for resume {resume_id}/{total_count} (batch: {batch_id})")

        # Publish processing start message
        publish_message(status_channel, json.dumps({
            "action": "struct/status",
            "message": f"Processing Resume {resume_id}/{total_count}",
            "resume_id": resume_id,
            "batch_id": batch_id,
            "progress": 0
        }))

        # Log before LLM call
        # logger.info(f"📄 Calling LLM for resume {resume_id}")

        # Main processing - LLM call
        resume_result = extract_profile_data(resume_text)

        processing_time = time.time() - start_time
        # logger.info(f"✅ LLM processing completed for resume {resume_id} in {processing_time:.2f}s")
        # logger.info(f"📊 Result: {len(resume_result.get('resume_score', []))} criteria evaluated")

        # Cache the result (expires in 1 hour)
        redis_client.setex(result_key, 3600, json.dumps(resume_result))

        # Remove processing lock
        redis_client.delete(processing_key)

        # Atomically increment progress and check completion
        current_count, is_complete = increment_batch_progress(
            batch_id, total_count, redis_client)

        # Publish the cached result
        publish_message(status_channel, json.dumps({
            "action": "struct/result",
            "resume_id": resume_id,
            "batch_id": batch_id,
            "resume_result": resume_result,
            "processing_time": time.time() - start_time,
            "cached": False,
            "batch_progress": f"{current_count}/{total_count}"
        }))

        logger.info(f"📤 Results published for resume {resume_id}")

        return {
            "resume_result": resume_result,
            "processing_time": processing_time,
            "cached": False,
            "batch_progress": f"{current_count}/{total_count}"
        }

    except Exception as e:
        processing_time = time.time() - start_time

        # Remove processing lock on error
        redis_client.delete(processing_key)

        # logger.error(f"❌ Error processing resume {resume_id}: {str(e)}")
        # logger.error(f"📋 Error details: {traceback.format_exc()}")

        # Publish error message
        publish_message(status_channel, json.dumps({
            "action": "struct/error",
            "message": f"Failed to process resume {resume_id}: {str(e)}",
            "resume_id": resume_id,
            "batch_id": batch_id,
            "processing_time": processing_time,
            "error_type": type(e).__name__
        }))

        # Retry logic for transient errors
        if self.request.retries < self.max_retries:
            logger.warning(
                f"🔄 Retrying resume {resume_id} (attempt {self.request.retries + 1})")
            raise self.retry(countdown=60 * (self.request.retries + 1))

        # Final failure after all retries
        logger.error(
            f"💀 Final failure for resume {resume_id} after {self.request.retries} retries")
        raise

    finally:
        # Cleanup processing lock if still exists
        if redis_client.exists(processing_key):
            redis_client.delete(processing_key)

        logger.info(f"🏁 Task completed for resume {resume_id}")


@celery_app.task
def cleanup_batch_progress(batch_id):
    """Clean up batch progress tracking data"""
    redis_client = get_redis_client()
    progress_key = f"batch_progress:{batch_id}"

    if redis_client.exists(progress_key):
        redis_client.delete(progress_key)
        logger.info(f"🧹 Cleaned up progress tracking for batch {batch_id}")
