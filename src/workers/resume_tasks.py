from src.celery_config import celery_app
from src.call_model.resume_analysis_model import evaluate_resume_sync
from src.utils.redis_pubsub import publish_message
import json
import asyncio


@celery_app.task
def process_resume_task(resume_text, parameters, index, total):
    status_channel = "resume_status"

    # Publish processing start message
    publish_message(status_channel, json.dumps({
        "action": "status",
        "message": f"Processing Resume {index}/{total}",
        "progress": int((index/total)*100)
    }))

    try:
        resume_result = evaluate_resume_sync(resume_text, parameters)
        # user_details = asyncio.run(getCandidateDetails(resume_text))
    except Exception as e:
        publish_message(status_channel, json.dumps({
            "action": "error",
            "message": str(e),
            "current": index,
            "progress": int((index / total) * 100)
        }))
        raise
    publish_message(status_channel, json.dumps({
        "action": "result",
        "resume_result": resume_result,
        # "user_details": user_details,
        "progress": int((index/total)*100)
    }))

    return {
        "resume_result": resume_result,
        # "user_details": user_details
    }
