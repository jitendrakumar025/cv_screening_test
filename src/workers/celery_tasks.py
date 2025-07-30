from src.celery_config import celery_app
# 👇 Import the synchronous functions
from src.call_model.resume_analysis_model import evaluate_resume_sync
from src.call_model.resume_structured_model import extract_profile_data_sync
from src.call_model.talentPool_analysis_model import evaluate_talentPool_sync
from src.utils.redis_pubsub import publish_message, get_redis_client, increment_batch_progress
import json
import logging
import traceback
import hashlib
import time
import threading

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def generate_resume_hash(resume_text: str, parameters: dict) -> str:
    """Generate unique hash for resume+parameters combination to prevent duplicates"""
    content = f"{resume_text}||{json.dumps(parameters, sort_keys=True)}"
    return hashlib.md5(content.encode()).hexdigest()


# 👇 Task is a standard synchronous function again
@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def process_resume_task(self, resume_text, parameters, resume_id, batch_id, total_count, candidate_id, round_id):
    thread_id = threading.get_ident()
    logger.info(f"[Thread ID: {thread_id}] 🚀 STARTING task for resume {resume_id}.")
    
    redis_client = get_redis_client()
    status_channel = f"resume-status_{batch_id}"
    resume_hash = generate_resume_hash(resume_text, parameters)
    processing_key = f"processing:{resume_hash}"
    result_key = f"result:{resume_hash}"
    start_time = time.time()

    try:
        if redis_client.exists(result_key):
            cached_result = json.loads(redis_client.get(result_key))
            # Increment progress for cached results to ensure batch completion
            # current_count, is_complete = increment_batch_progress(batch_id, total_count, redis_client)
            publish_message(status_channel, json.dumps({
                "processing_time": 0, "cached": True, "batch_progress": f"cached",
                "action": "analysis/result", "resume_id": resume_id, "candidate_id": candidate_id,
                "round_id": round_id, "batch_id": batch_id, "resume_result": cached_result,
            }))
            return {"resume_result": cached_result, "cached": True}

        is_set = redis_client.set(processing_key, "processing", nx=True, ex=600)
        if not is_set:
            logger.info(f"🔄 Resume {resume_id} already being processed, retrying...")
            raise self.retry(countdown=10)
        
        publish_message(status_channel, json.dumps({
            "action": "analysis/status", "message": f"Processing Resume {resume_id}/{total_count}",
            "resume_id": resume_id, "batch_id": batch_id, "candidate_id": candidate_id,
            "round_id": round_id, "progress": 0
        }))
        
        logger.info(f"[Thread ID: {thread_id}] 📞 Calling LLM for resume {resume_id}. Waiting...")
        # 👇 Direct synchronous call
        resume_result = evaluate_resume_sync(resume_text, parameters)
        logger.info(f"[Thread ID: {thread_id}] ✅ LLM call COMPLETE for resume {resume_id}.")
        
        processing_time = time.time() - start_time
        redis_client.setex(result_key, 3600, json.dumps(resume_result))
        
        current_count, is_complete = increment_batch_progress(batch_id, total_count, redis_client)
        
        publish_message(status_channel, json.dumps({
            "processing_time": processing_time, "cached": False, "batch_progress": f"{current_count}/{total_count}",
            "action": "analysis/result", "resume_id": resume_id, "batch_id": batch_id,
            "candidate_id": candidate_id, "round_id": round_id, "resume_result": resume_result,
        }))
        
        return {
            "resume_result": resume_result, "processing_time": processing_time,
            "cached": False, "batch_progress": f"{current_count}/{total_count}"
        }
    except Exception as e:
        logger.error(f"❌ Error processing resume {resume_id}: {e}", exc_info=True)
        # Ensure progress is still incremented on final failure to unblock the batch
        if self.request.retries >= self.max_retries:
             increment_batch_progress(batch_id, total_count, redis_client)
        raise
    finally:
        logger.info(f"[Thread ID: {thread_id}] 🏁 FINISHED task for resume {resume_id}.")
        redis_client.delete(processing_key)


# 👇 Task is a standard synchronous function again
@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def struct_resume_task(self, resume_text, resume_id, batch_id, total_count):
    thread_id = threading.get_ident()
    redis_client = get_redis_client()
    status_channel = f"resume-status_{batch_id}"
    resume_hash = generate_resume_hash(resume_text, resume_id)
    processing_key = f"processing:{resume_hash}"
    result_key = f"result:{resume_hash}"
    start_time = time.time()

    try:
        if redis_client.exists(result_key):
            cached_result = json.loads(redis_client.get(result_key))
            # current_count, is_complete = increment_batch_progress(batch_id, total_count, redis_client)
            logger.info(f"Resume with {resume_id} already cached! ")
            publish_message(status_channel, json.dumps({
                "cached": True, "batch_progress": f"cached",
                "action": "struct/result", "resume_id": resume_id, "batch_id": batch_id,
                "resume_result": cached_result, "processing_time": 0,
            }))
            return {"resume_result": cached_result, "cached": True}

        is_set = redis_client.set(processing_key, "processing", nx=True, ex=600)
        if not is_set:
            logger.info(f"🔄 Resume {resume_id} already being processed, retrying...")
            raise self.retry(countdown=10)

        publish_message(status_channel, json.dumps({
            "action": "struct/status", "message": f"Processing Resume {resume_id}/{total_count}",
            "resume_id": resume_id, "batch_id": batch_id, "progress": 0
        }))

        # logger.info(f"[Thread ID: {thread_id}] 📞 Calling LLM for resume {resume_id}. Waiting...")
        # 👇 Direct synchronous call
        resume_result = extract_profile_data_sync(resume_text)
        # logger.info(f"[Thread ID: {thread_id}] ✅ LLM call COMPLETE for resume {resume_id}.")
        
        processing_time = time.time() - start_time
        redis_client.setex(result_key, 300, json.dumps(resume_result))
        
        current_count, is_complete = increment_batch_progress(batch_id, total_count, redis_client)
        
        publish_message(status_channel, json.dumps({
            "action": "struct/result","batch_progress": f"{current_count}/{total_count}", "resume_id": resume_id, "batch_id": batch_id,
            "resume_result": resume_result, "processing_time": processing_time,
            "cached": False
        }))
        
        return {
            "batch_progress": f"{current_count}/{total_count}",
            "resume_result": resume_result, "processing_time": processing_time,
            "cached": False, 
        }
    
    except Exception as e:
        logger.error(f"❌ Error processing resume {resume_id}: {e}", exc_info=True)
        if self.request.retries >= self.max_retries:
             increment_batch_progress(batch_id, total_count, redis_client)
        raise
    finally:
        # logger.info(f"[Thread ID: {thread_id}] 🏁 FINISHED task for resume {resume_id}.")
        redis_client.delete(processing_key)


# 👇 Task is a standard synchronous function again
@celery_app.task(bind=True, max_retries=3, default_retry_delay=60, ignore_result=True)
def pool_analysis_task(self, profile_analysis, parameters, profile_id, batch_id, total_count):
    redis_client = get_redis_client()
    status_channel = f"pool-status_{batch_id}"
    logger.info(f"🚀 Starting processing for profile {profile_id} (batch: {batch_id})")
    start_time = time.time()
    try:
        publish_message(status_channel, json.dumps({
            "action": "pool/status", "message": f"Processing Profile {profile_id}/{total_count}",
            "profile_id": profile_id, "batch_id": batch_id, "progress": 0
        }))

        # 👇 Direct synchronous call
        pool_profile_result = evaluate_talentPool_sync(json.dumps(profile_analysis), parameters)

        processing_time = time.time() - start_time
        current_count, is_complete = increment_batch_progress(batch_id, total_count, redis_client)

        publish_message(status_channel, json.dumps({
            "action": "analysis/result", "profile_id": profile_id,
            "pool_result": pool_profile_result, "processing_time": processing_time,
            "cached": False, "batch_progress": f"{current_count}/{total_count}"
        }))
        logger.info(f"📤 Results published for profile {profile_id}")
    except Exception as e:
        logger.error(f"❌ Error processing profile {profile_id}: {e}", exc_info=True)
        if self.request.retries >= self.max_retries:
             increment_batch_progress(batch_id, total_count, redis_client)
        raise
    finally:
        logger.info(f"🏁 Task completed for profile {profile_id}")
