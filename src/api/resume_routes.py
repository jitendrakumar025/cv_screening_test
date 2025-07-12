from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from src.workers.celery_tasks import process_resume_task, struct_resume_task,pool_analysis_task
from src.utils.redis_pubsub import get_redis_client

from typing import List, Dict, Any
import uuid
import logging

logger = logging.getLogger(__name__)
router = APIRouter()


class ResumeItem(BaseModel):
    resume_id: str
    resume_text: str
    candidate_id: str
    round_id: str


class ResumeAnalysisRequest(BaseModel):
    resume_list: List[ResumeItem]
    parameters: Dict[str, Any]
    batch_name: str = None
    batch_id: str = None  


class BatchStatusResponse(BaseModel):
    batch_id: str
    total_count: int
    message: str
    sse_channel: str


def reset_batch_progress(batch_id: str):
    """Reset batch progress counters for a fresh start"""
    try:
        redis_client = get_redis_client()
        progress_key = f"batch_progress:{batch_id}"
        completion_key = f"batch_completed:{batch_id}"

        # Delete existing progress and completion markers
        redis_client.delete(progress_key, completion_key)
        logger.info(f"🧹 Reset batch progress for {batch_id}")

    except Exception as e:
        logger.error(f"Error resetting batch progress: {e}")


@router.post("/start-resume-analysis", response_model=BatchStatusResponse)
def start_resume_analysis(payload: ResumeAnalysisRequest):
    """
    Start processing a batch of resumes with optimized concurrency
    """
    try:
        resume_list = payload.resume_list
        parameters = payload.parameters
        batch_name = payload.batch_name or "resume_batch"
        batch_id = payload.batch_id

        if not resume_list:
            raise HTTPException(
                status_code=400, detail="Resume list cannot be empty")

        if len(resume_list) > 1000:  # Safety limit
            raise HTTPException(
                status_code=400, detail="Maximum 1000 resumes per batch")

        # Generate unique batch ID
        # batch_id = f"{batch_name}_{uuid.uuid4().hex[:8]}"
        total_count = len(resume_list)

        logger.info(f"🚀 Starting batch {batch_id} with {total_count} resumes")

        # Dispatch all resume processing tasks
        task_ids = []
        for index, resume_item in enumerate(resume_list, start=1):
            # Create unique resume ID within batch
            resume_id = resume_item.resume_id
            resume_text = resume_item.resume_text
            # Dispatch task to Celery
            task = process_resume_task.delay(
                resume_text=resume_text,
                parameters=parameters,
                resume_id=resume_id,
                batch_id=batch_id,
                total_count=total_count,
                candidate_id=resume_item.candidate_id,
                round_id=resume_item.round_id,
            )
            task_ids.append(task.id)

        logger.info(f"📤 Dispatched {len(task_ids)} tasks for batch {batch_id}")

        return BatchStatusResponse(
            batch_id=batch_id,
            total_count=total_count,
            message=f"Resume processing batch '{batch_id}' dispatched successfully with {total_count} resumes",
            sse_channel=f"resume_status_{batch_id}"
        )

    except Exception as e:
        logger.error(f"Error starting resume analysis: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to start analysis: {str(e)}")



class ResumeStructItem(BaseModel):
    resume_id: str
    resume_text: str

class ResumeStructRequest(BaseModel):
    resume_list: List[ResumeStructItem]
    batch_name: str = None
    batch_id: str = None  

@router.post("/start-resume-structuring", response_model=BatchStatusResponse)
def start_resume_structuring(payload: ResumeStructRequest):
    """
    Start processing a batch of resumes with optimized concurrency to get structured resume
    """

    try:
        resume_list = payload.resume_list
        batch_name = payload.batch_name or "resume_batch"
        batch_id = payload.batch_id

        print(f"🚀 Starting batch {batch_id} with {len(resume_list)} resumes")

        if not resume_list:
            raise HTTPException(
                status_code=400, detail="Resume list cannot be empty")

        if len(resume_list) > 1000:  # Safety limit
            raise HTTPException(
                status_code=400, detail="Maximum 1000 resumes per batch")

        # Generate unique batch ID
        # batch_id = f"{batch_name}_{uuid.uuid4().hex[:8]}"
        total_count = len(resume_list)

        # Reset batch progress to ensure clean start
        reset_batch_progress(batch_id)

        logger.info(f"🚀 Starting batch {batch_id} with {total_count} resumes")

        # Dispatch all resume processing tasks
        task_ids = []
        for index, resume_item in enumerate(resume_list, start=1):
            # Create unique resume ID within batch
            resume_id = resume_item.resume_id
            resume_text = resume_item.resume_text
            # Dispatch task to Celery
            task = struct_resume_task.delay(
                resume_text=resume_text,
                resume_id=resume_id,
                batch_id=batch_id,
                total_count=total_count
            )
            task_ids.append(task.id)

        logger.info(f"📤 Dispatched {len(task_ids)} tasks for batch {batch_id}")

        return BatchStatusResponse(
            batch_id=batch_id,
            total_count=total_count,
            message=f"Resume processing batch '{batch_id}' dispatched successfully with {total_count} resumes",
            sse_channel=f"resume_status_{batch_id}"
        )

    except Exception as e:
        logger.error(f"Error starting resume analysis: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to start analysis: {str(e)}")

class InputItem(BaseModel):
    pool_profile:Dict[str,Any]
    analysis:Dict[str,Any]
class TalentPoolStructRequest(BaseModel):
    profile_analysis_list: List[InputItem]
    parameters: List[Dict[str, Any]]
    batch_name: str = None
    batch_id: str = None  

@router.post("/start-talentPool-analysis", response_model=BatchStatusResponse)
def start_talentPool_analysis(payload: TalentPoolStructRequest):
    """
    Start processing a batch of talentpools with optimized concurrency to get analysed result
    """

    try:
        profile_analysis_list = payload.profile_analysis_list
        batch_name = payload.batch_name or "pool_batch"
        batch_id = payload.batch_id
        parameters=payload.parameters

        print(f"🚀 Starting batch {batch_id} with {len(profile_analysis_list)} pool_profiles")

        if not profile_analysis_list:
            raise HTTPException(
                status_code=400, detail="pool_profiles list cannot be empty")

        if len(profile_analysis_list) > 1000:  # Safety limit
            raise HTTPException(
                status_code=400, detail="Maximum 1000 pool_profiles per batch")

        # Generate unique batch ID
        # batch_id = f"{batch_name}_{uuid.uuid4().hex[:8]}"
        total_count = len(profile_analysis_list)

        # Reset batch progress to ensure clean start
        reset_batch_progress(batch_id)

        logger.info(f"🚀 Starting batch {batch_id} with {total_count} pool_profiles")

        # Dispatch all resume processing tasks
        task_ids = []
        for index, item in enumerate(profile_analysis_list, start=1):
            # Create unique resume ID within batch
            profile_id = item.pool_profile["id"]
            profile_analysis = item.analysis
            # Dispatch task to Celery
            task = pool_analysis_task.delay(
                profile_analysis=profile_analysis,
                parameters=parameters,
                profile_id=profile_id,
                batch_id=batch_id,
                total_count=total_count
            )
            task_ids.append(task.id)

        logger.info(f"📤 Dispatched {len(task_ids)} tasks for batch {batch_id}")

        return BatchStatusResponse(
            batch_id=batch_id,
            total_count=total_count,
            message=f"pool_profile processing batch '{batch_id}' dispatched successfully with {total_count} profiles",
            sse_channel=f"pool_status_{batch_id}"
        )

    except Exception as e:
        logger.error(f"Error starting pool_profiles analysis: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to start analysis: {str(e)}")


@router.get("/batch-status/{batch_id}")
def get_batch_status(batch_id: str):
    """
    Get current status of a processing batch
    """
    try:
        from src.utils.redis_pubsub import get_batch_progress

        completed_count = get_batch_progress(batch_id)

        return {
            "batch_id": batch_id,
            "completed_count": completed_count,
            "sse_channel": f"resume_status_{batch_id}",
            # Adjust based on your batch size
            "status": "processing" if completed_count < 1000 else "completed"
        }

    except Exception as e:
        logger.error(f"Error getting batch status: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to get status: {str(e)}")


@router.delete("/cleanup-batch/{batch_id}")
def cleanup_batch(batch_id: str):
    """
    Clean up batch processing data
    """
    try:
        from src.utils.redis_pubsub import cleanup_batch_data

        cleanup_batch_data(batch_id)

        return {
            "batch_id": batch_id,
            "message": "Batch data cleaned up successfully"
        }

    except Exception as e:
        logger.error(f"Error cleaning up batch: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to cleanup: {str(e)}")
