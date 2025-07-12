from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from src.utils.redis_pubsub import subscribe_to_channel
import json
import logging

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/stream/resume-status/{batch_id}")
def stream_resume_status_by_batch(batch_id: str):
    """
    Stream resume processing status for a specific batch
    """
    def event_stream():
        pubsub = None
        try:
            channel = f"resume_status_{batch_id}"
            pubsub = subscribe_to_channel(channel)
            print(f"🔗 Subscribed to channel: {channel} for batch {batch_id}")
            # Send initial connection confirmation
            yield f"data: {json.dumps({'action': 'connected', 'batch_id': batch_id, 'channel': channel})}\n\n"
            
            for message in pubsub.listen():
                if message['type'] == 'message':
                    try:
                        # Parse message to add metadata
                        data = json.loads(message['data'])
                        data['timestamp'] = int(__import__('time').time())
                        yield f"data: {json.dumps(data)}\n\n"
                    except json.JSONDecodeError:
                        # If parsing fails, send raw message
                        yield f"data: {message['data']}\n\n"
        except GeneratorExit:
            # Client disconnected, clean up
            print(f"❌ Client disconnected from SSE stream for batch {batch_id}")                  
        except Exception as e:
            logger.error(f"SSE stream error for batch {batch_id}: {str(e)}")
            error_msg = {
                'action': 'error',
                'message': f'Stream error: {str(e)}',
                'batch_id': batch_id
            }
            yield f"data: {json.dumps(error_msg)}\n\n"
        finally:
            if pubsub:
                try:
                    pubsub.close()
                except:
                    pass
    
    return StreamingResponse(
        event_stream(), 
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Cache-Control"
        }
    )
@router.get("/stream/pool-status/{batch_id}")
def stream_pool_status_by_batch(batch_id: str):
    """
    Stream pool profile processing status for a specific batch
    """
    def event_stream():
        pubsub = None
        try:
            channel = f"pool_status_{batch_id}"
            pubsub = subscribe_to_channel(channel)
            print(f"🔗 Subscribed to channel: {channel} for batch {batch_id}")
            # Send initial connection confirmation
            yield f"data: {json.dumps({'action': 'connected', 'batch_id': batch_id, 'channel': channel})}\n\n"
            
            for message in pubsub.listen():
                if message['type'] == 'message':
                    try:
                        # Parse message to add metadata
                        data = json.loads(message['data'])
                        data['timestamp'] = int(__import__('time').time())
                        yield f"data: {json.dumps(data)}\n\n"
                    except json.JSONDecodeError:
                        # If parsing fails, send raw message
                        yield f"data: {message['data']}\n\n"
        except GeneratorExit:
            # Client disconnected, clean up
            print(f"❌ Client disconnected from SSE stream for batch {batch_id}")                
        except Exception as e:
            logger.error(f"SSE stream error for batch {batch_id}: {str(e)}")
            error_msg = {
                'action': 'error',
                'message': f'Stream error: {str(e)}',
                'batch_id': batch_id
            }
            yield f"data: {json.dumps(error_msg)}\n\n"
        finally:
            if pubsub:
                try:
                    pubsub.close()
                except:
                    pass
    
    return StreamingResponse(
        event_stream(), 
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Cache-Control"
        }
    )

@router.get("/stream/resume-status")
def stream_resume_status():
    """
    Legacy endpoint - streams all resume status updates
    """
    def event_stream():
        pubsub = None
        try:
            pubsub = subscribe_to_channel("resume_status")
            
            for message in pubsub.listen():
                if message['type'] == 'message':
                    try:
                        data = json.loads(message['data'])
                        data['timestamp'] = int(__import__('time').time())
                        yield f"data: {json.dumps(data)}\n\n"
                    except json.JSONDecodeError:
                        yield f"data: {message['data']}\n\n"
        except GeneratorExit:
            # Client disconnected, clean up
            print(f"❌ Client disconnected from SSE stream")                  
        except Exception as e:
            logger.error(f"SSE stream error: {str(e)}")
            error_msg = {
                'action': 'error',
                'message': f'Stream error: {str(e)}'
            }
            yield f"data: {json.dumps(error_msg)}\n\n"
        finally:
            if pubsub:
                try:
                    pubsub.close()
                except:
                    pass
    
    return StreamingResponse(
        event_stream(), 
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive"
        }
    )