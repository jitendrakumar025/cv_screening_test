from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from src.utils.redis_pubsub import subscribe_to_channel
import json

router = APIRouter()

@router.get("/stream/resume-status")
def stream_resume_status():
    def event_stream():
        pubsub = subscribe_to_channel("resume_status")
        for message in pubsub.listen():
            if message['type'] == 'message':
                yield f"data: {message['data']}\n\n"
    return StreamingResponse(event_stream(), media_type="text/event-stream")
