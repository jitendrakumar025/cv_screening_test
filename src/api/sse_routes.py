from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
# Import the function that returns the pooled client
from src.utils.redis_pubsub import get_redis_client 
import json
import logging
import asyncio

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/stream/resume-status/{batch_id}")
async def stream_resume_status_by_batch(request: Request, batch_id: str):
    """
    Stream resume processing status for a specific batch.
    This async implementation correctly handles client disconnections 
    and uses the shared Redis connection pool efficiently.
    """
    
    # Get the single, pooled Redis client instance
    redis_client = get_redis_client()

    async def event_stream():
        # Create a new pubsub object for this specific client request
        pubsub = redis_client.pubsub(ignore_subscribe_messages=True)
        channel = f"resume_status_{batch_id}"
        
        try:
            # Subscribe to the channel
            await pubsub.subscribe(channel)
            logger.info(f"✅ Client connected and subscribed to channel: {channel}")

            # Send an initial connection confirmation message
            yield f"data: {json.dumps({'action': 'connected', 'batch_id': batch_id})}\n\n"

            while True:
                # 1. Check if the client has disconnected
                if await request.is_disconnected():
                    logger.warning(f"🔌 Client disconnected from channel: {channel}. Closing stream.")
                    break

                # 2. Get a message from Redis with a timeout
                # This is now an async call
                message = await pubsub.get_message(timeout=1.0)

                if message is None:
                    # If no message, send a keep-alive comment or just continue
                    yield ": keep-alive\n\n"
                    continue
                
                # 3. Process and send the message
                logger.debug(f"Message received on {channel}: {message['data']}")
                try:
                    # The connection pool is set to decode_responses=True, so no .decode() needed
                    data = json.loads(message['data'])
                    yield f"data: {json.dumps(data)}\n\n"
                except json.JSONDecodeError:
                    # Fallback for non-JSON data
                    yield f"data: {message['data']}\n\n"
                
                # Small sleep to prevent a tight loop if messages are arriving very fast
                await asyncio.sleep(0.01)

        except asyncio.CancelledError:
            # This happens when the client disconnects
            logger.info(f"Stream cancelled for channel: {channel}")
        except Exception as e:
            logger.error(f"SSE stream error on channel {channel}: {e}", exc_info=True)
            # Optionally send an error to the client
            error_msg = json.dumps({'action': 'error', 'message': str(e)})
            yield f"data: {error_msg}\n\n"
        finally:
            # CRITICAL: Unsubscribe and close the pubsub client
            # This returns the connection to the pool.
            logger.info(f"❌ Unsubscribing and closing pubsub for channel: {channel}")
            await pubsub.close()

    return StreamingResponse(event_stream(), media_type="text/event-stream")


# @router.get("/stream/pool-status/{batch_id}")
# def stream_pool_status_by_batch(batch_id: str):
#     """
#     Stream pool profile processing status for a specific batch
#     """
#     def event_stream():
#         pubsub = None
#         try:
#             channel = f"pool_status_{batch_id}"
#             pubsub = subscribe_to_channel(channel)
#             print(f"🔗 Subscribed to channel: {channel} for batch {batch_id}")
#             # Send initial connection confirmation
#             yield f"data: {json.dumps({'action': 'connected', 'batch_id': batch_id, 'channel': channel})}\n\n"
            
#             for message in pubsub.listen():
#                 if message['type'] == 'message':
#                     try:
#                         # Parse message to add metadata
#                         data = json.loads(message['data'])
#                         data['timestamp'] = int(__import__('time').time())
#                         yield f"data: {json.dumps(data)}\n\n"
#                     except json.JSONDecodeError:
#                         # If parsing fails, send raw message
#                         yield f"data: {message['data']}\n\n"
#         except GeneratorExit:
#             # Client disconnected, clean up
#             print(f"❌ Client disconnected from SSE stream for batch {batch_id}")                
#         except Exception as e:
#             logger.error(f"SSE stream error for batch {batch_id}: {str(e)}")
#             error_msg = {
#                 'action': 'error',
#                 'message': f'Stream error: {str(e)}',
#                 'batch_id': batch_id
#             }
#             yield f"data: {json.dumps(error_msg)}\n\n"
#         finally:
#             if pubsub:
#                 try:
#                     pubsub.close()
#                 except:
#                     pass
    
#     return StreamingResponse(
#         event_stream(), 
#         media_type="text/event-stream",
#         headers={
#             "Cache-Control": "no-cache",
#             "Connection": "keep-alive",
#             "Access-Control-Allow-Origin": "*",
#             "Access-Control-Allow-Headers": "Cache-Control"
#         }
#     )

# @router.get("/stream/resume-status")
# def stream_resume_status():
#     """
#     Legacy endpoint - streams all resume status updates
#     """
#     def event_stream():
#         pubsub = None
#         try:
#             pubsub = subscribe_to_channel("resume_status")
            
#             for message in pubsub.listen():
#                 if message['type'] == 'message':
#                     try:
#                         data = json.loads(message['data'])
#                         data['timestamp'] = int(__import__('time').time())
#                         yield f"data: {json.dumps(data)}\n\n"
#                     except json.JSONDecodeError:
#                         yield f"data: {message['data']}\n\n"
#         except GeneratorExit:
#             # Client disconnected, clean up
#             print(f"❌ Client disconnected from SSE stream")                  
#         except Exception as e:
#             logger.error(f"SSE stream error: {str(e)}")
#             error_msg = {
#                 'action': 'error',
#                 'message': f'Stream error: {str(e)}'
#             }
#             yield f"data: {json.dumps(error_msg)}\n\n"
#         finally:
#             if pubsub:
#                 try:
#                     pubsub.close()
#                 except:
#                     pass
    
#     return StreamingResponse(
#         event_stream(), 
#         media_type="text/event-stream",
#         headers={
#             "Cache-Control": "no-cache",
#             "Connection": "keep-alive"
#         }
#     )