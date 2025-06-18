from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from src.workers.resume_tasks import process_resume_task, process_batch_summary
from typing import List, Dict, Any
import uuid
import logging

logger = logging.getLogger(__name__)
router = APIRouter()

class ResumeAnalysisRequest(BaseModel):
    resume_list: List[str]
    parameters: Dict[str, Any]
    batch_name: str = None

class BatchStatusResponse(BaseModel):
    batch_id: str
    total_count: int
    message: str
    sse_channel: str

@router.post("/start-resume-analysis", response_model=BatchStatusResponse)
def start_resume_analysis(payload: ResumeAnalysisRequest):
    """
    Start processing a batch of resumes with optimized concurrency
    """
    try:
        resume_list = payload.resume_list
        parameters = payload.parameters
        batch_name = payload.batch_name or "resume_batch"
        
        if not resume_list:
            raise HTTPException(status_code=400, detail="Resume list cannot be empty")
        
        if len(resume_list) > 2000:  # Safety limit
            raise HTTPException(status_code=400, detail="Maximum 2000 resumes per batch")
        
        # Generate unique batch ID
        batch_id = f"{batch_name}_{uuid.uuid4().hex[:8]}"
        total_count = len(resume_list)
        
        logger.info(f"🚀 Starting batch {batch_id} with {total_count} resumes")
        
        # Dispatch all resume processing tasks
        task_ids = []
        for index, resume_text in enumerate(resume_list, start=1):
            # Create unique resume ID within batch
            resume_id = f"{batch_id}_resume_{index}"
            
            # Dispatch task to Celery
            task = process_resume_task.delay(
                resume_text=resume_text,
                parameters=parameters,
                resume_id=resume_id,
                batch_id=batch_id,
                total_count=total_count
            )
            task_ids.append(task.id)
        
        # Schedule batch summary task (runs after a delay to allow processing)
        process_batch_summary.apply_async(
            args=[batch_id, total_count],
            countdown=10  # Wait 10 seconds before sending summary
        )
        
        logger.info(f"📤 Dispatched {len(task_ids)} tasks for batch {batch_id}")
        
        return BatchStatusResponse(
            batch_id=batch_id,
            total_count=total_count,
            message=f"Resume processing batch '{batch_id}' dispatched successfully with {total_count} resumes",
            sse_channel=f"resume_status_{batch_id}"
        )
        
    except Exception as e:
        logger.error(f"Error starting resume analysis: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to start analysis: {str(e)}")

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
            "status": "processing" if completed_count < 1000 else "completed"  # Adjust based on your batch size
        }
        
    except Exception as e:
        logger.error(f"Error getting batch status: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get status: {str(e)}")

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
        raise HTTPException(status_code=500, detail=f"Failed to cleanup: {str(e)}")