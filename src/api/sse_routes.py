# src/api/sse_routes.py

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
# 👇 Import the new ASYNC function
from src.utils.async_redis import get_async_redis_client
import json
import logging
import asyncio

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/stream/{channel_type}/{batch_id}")
async def stream_status_by_batch(request: Request, channel_type: str, batch_id: str):
    """
    Stream processing status for a specific batch for various task types.
    This async implementation correctly handles client disconnections 
    and uses the shared ASYNC Redis connection pool efficiently.
    
    Valid channel_types: 'resume-status', 'pool-status'
    """
    
    # 👇 Get the single, pooled ASYNC Redis client instance
    try:
        redis_client = get_async_redis_client()
    except ConnectionError as e:
        logger.error(f"Failed to get Redis client for SSE stream: {e}")
        # Return an error response or handle appropriately
        return StreamingResponse(iter(["data: {\"action\": \"error\", \"message\": \"Could not connect to stream server.\"}\n\n"]), media_type="text/event-stream")

    # Construct channel name based on type
    if channel_type not in ["resume-status", "pool-status"]:
         return StreamingResponse(iter([f"data: {json.dumps({'action': 'error', 'message': 'Invalid channel type specified.'})}\n\n"]), media_type="text/event-stream")
    
    channel = f"{channel_type}_{batch_id}"

    async def event_stream():
        # Create a new pubsub object for this specific client request
        pubsub = redis_client.pubsub(ignore_subscribe_messages=True)
        
        try:
            # Subscribe to the channel
            await pubsub.subscribe(channel)
            logger.info(f"✅ Client connected and subscribed to channel: {channel}")

            # Send an initial connection confirmation message
            yield f"data: {json.dumps({'action': 'connected', 'batch_id': batch_id, 'channel': channel})}\n\n"

            while True:
                # 1. Check if the client has disconnected
                if await request.is_disconnected():
                    logger.warning(f"🔌 Client disconnected from channel: {channel}. Closing stream.")
                    break

                # 2. Get a message from Redis with a timeout
                message = await pubsub.get_message(timeout=1.0)

                if message is None:
                    # If no message, send a keep-alive comment to prevent timeouts
                    yield ": keep-alive\n\n"
                    continue
                
                # 3. Process and send the message
                logger.debug(f"Message received on {channel}: {message['data']}")
                yield f"data: {message['data']}\n\n"

        except asyncio.CancelledError:
            # This happens when the client disconnects
            logger.info(f"Stream cancelled for channel: {channel}")
        except Exception as e:
            logger.error(f"SSE stream error on channel {channel}: {e}", exc_info=True)
            error_msg = json.dumps({'action': 'error', 'message': str(e)})
            yield f"data: {error_msg}\n\n"
        finally:
            # CRITICAL: Unsubscribe and close the pubsub client
            logger.info(f"❌ Unsubscribing and closing pubsub for channel: {channel}")
            await pubsub.close()

    return StreamingResponse(event_stream(), media_type="text/event-stream")
