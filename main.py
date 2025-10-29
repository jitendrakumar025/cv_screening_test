import asyncio
from datetime import datetime, timezone
import json
from click.core import batch
import redis.asyncio as redis
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from typing import Optional, Dict


import os
from dotenv import load_dotenv
import logging
from pydantic import BaseModel


from tasks import struct_resume_task, analyze_resume_task, pool_analysis_task

load_dotenv()
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
MAX_WEBSOCKET_CONNECTIONS = int(os.getenv("MAX_WEBSOCKET_CONNECTIONS", "1000"))


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


redis_pool = redis.ConnectionPool.from_url(
    REDIS_URL,
    max_connections=200,
    socket_keepalive=True,
    socket_connect_timeout=5,
    health_check_interval=30,
)

# websocket connection tracking

active_connections: Dict[str, set] = {}
connection_count = 0


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize resources on startup and cleanup on shutdown."""
    logger.info("Starting Resume Processing API")
    # Test Redis connection
    try:
        r = redis.Redis(connection_pool=redis_pool)
        await r.ping()
        await r.aclose()
        logger.info("Redis connection successful")
    except Exception as e:
        logger.error(f"Redis connection failed: {e}")
        raise

    yield

    # Cleanup on shutdown
    logger.info("Shutting down Resume Processing API")
    # Close all active WebSocket connections
    for batch_id, connections in active_connections.items():
        for ws in connections:
            try:
                await ws.close(code=1001, reason="Server shutting down")
            except Exception:
                pass
    active_connections.clear()


app = FastAPI(title="Resume Processing API", version="2.0.0", lifespan=lifespan)

# --- CORS Middleware ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class BatchStatusResponse(BaseModel):
    batch_id: str
    total_count: int
    message: str
    sse_channel: str


class BatchRequest(BaseModel):
    resume_list: list
    batch_id: Optional[str] = None


@app.get("/")
async def root():
    return {
        "message": "Welcome to the Resume Processing API!",
        "version": "2.0.0",
        "endpoints": {
            "health": "/health",
            "metrics": "/metrics",
            "websocket": "/ws/stream",
            "start_structuring": "/start-resume-structuring",
            "start_analysis": "/start-resume-analysis",
            "start_pool_analysis": "/start-talentPool-analysis",
        },
    }


@app.get("/health")
async def check_health():
    """Health check endpoint with Redis connectivity test."""
    try:
        r = redis.Redis(connection_pool=redis_pool)
        await r.ping()
        await r.aclose()
        redis_status = "healthy"
    except Exception as e:
        logger.error(f"Redis health check failed: {e}")
        redis_status = "unhealthy"
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "redis": redis_status, "error": str(e)},
        )

    return {
        "status": "healthy",
        "redis": redis_status,
        "active_websockets": connection_count,
        "active_batches": len(active_connections),
    }


@app.get("/metrics")
async def get_metrics():
    """Prometheus-style metrics endpoint."""
    return {
        "active_websocket_connections": connection_count,
        "active_batches": len(active_connections),
        "max_connections_limit": MAX_WEBSOCKET_CONNECTIONS,
    }


@app.post("/start-resume-structuring", response_model=BatchStatusResponse)
async def start_resume_structuring(request: BatchRequest):
    """
    Accepts a list of resumes, creates a unique channel ID,
    and dispatches a Celery task for each resume.
    """
    try:
        # data = await request.json()
        # resumes_data = data.get("resume_list")
        # batch_id = data.get("batch_id")
        resumes_data = request.resume_list
        batch_id = request.batch_id

        # logger.info(f"Received resume list for batch_id:{batch_id}")
        if not resumes_data:
            raise HTTPException(status_code=400, detail="resume_list cannot be empty")

        if not all("resume_id" in r and "resume_text" in r for r in resumes_data):
            raise HTTPException(
                status_code=400,
                detail="Each resume must have 'resume_id' and 'resume_text'",
            )

        # Unique ID for this specific request, used as the Redis Pub/Sub channel
        channel_id = f"job_{batch_id}"

        # Dispatch a task for each resume, telling it which channel to publish to
        for resume in resumes_data:
            resume_id = resume.get("resume_id")
            resume_text = resume.get("resume_text")
            if resume_id and resume_text:
                struct_resume_task.delay(resume_id, resume_text, channel_id, batch_id)

        total_count = len(resumes_data)

        logger.info(f"Started structuring batch {batch_id} with {total_count} resumes")

        # Return the channel_id to the client so it knows where to listen
        return BatchStatusResponse(
            batch_id=batch_id,
            total_count=total_count,
            message=f"Resume processing batch '{batch_id}' dispatched successfully with {total_count} resumes",
            sse_channel=f"{channel_id}",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting structuring batch: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/start-resume-analysis", response_model=BatchStatusResponse)
async def start_resume_analysis(request: Request):
    """
    Accepts a list of resumes, creates a unique channel ID,
    and dispatches a Celery task for each resume.
    """

    try:

        data = await request.json()
        resumes_data = data.get("resume_list")
        batch_id = data.get("batch_id")
        parameters = data.get("parameters")


        if resumes_data is None or batch_id is None or parameters is None:
            logger.error(
                "Either Resume List or Batch Id,parameters is None! Internal Server Error!"
            )
            return ""

        if not isinstance(resumes_data, list) or not all(
            "resume_id" in r and "resume_text" in r for r in resumes_data
        ):
            return {
                "error": "Request must be a JSON list of objects, each with 'resume_id' and 'resume_text'."
            }

        # Unique ID for this specific request, used as the Redis Pub/Sub channel
        channel_id = f"job_{batch_id}"

        # Dispatch a task for each resume, telling it which channel to publish to
        for resume in resumes_data:
            resume_id = resume.get("resume_id")
            resume_text = resume.get("resume_text")
            candidate_id = resume.get("candidate_id")
            round_id = resume.get("round_id")
            if resume_id and resume_text:
                analyze_resume_task.delay(
                    resume_id,
                    resume_text,
                    channel_id,
                    batch_id,
                    candidate_id,
                    round_id,
                    parameters,
                )

        total_count = len(resumes_data)

        logger.info(
            f"Started job with channel ID: {channel_id} for {total_count} resumes."
        )

        # Return the channel_id to the client so it knows where to listen
        return BatchStatusResponse(
            batch_id=batch_id,
            total_count=total_count,
            message=f"Resume processing batch '{batch_id}' dispatched successfully with {total_count} resumes",
            sse_channel=f"{channel_id}",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting analysis batch: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/start-talentPool-analysis", response_model=BatchStatusResponse)
async def start_talentPool_analysis(request: Request):
    """
    Accepts a list of resumes, creates a unique channel ID,
    and dispatches a Celery task for each resume.
    """
    try:

        data = await request.json()
        profiles_data = data.get("profile_analysis_list")
        batch_id = data.get("batch_id")
        parameters = data.get("parameters")

        if profiles_data is None or batch_id is None or parameters is None:
            logger.error(
                "Either Profiles List or Batch Id,parameters is None! Internal Server Error!"
            )
            return ""

        # Unique ID for this specific request, used as the Redis Pub/Sub channel
        channel_id = f"job_{batch_id}"

        # Dispatch a task for each resume, telling it which channel to publish to
        for profile in profiles_data:
            profile_id = profile.pool_profile.get("id")
            profile_analysis = profile.get("analysis")
            if profile_id and profile_analysis:
                pool_analysis_task.delay(
                    candidate_profile=profile_analysis,
                    parameters=parameters,
                    profile_id=profile_id,
                    batch_id=batch_id,
                    channel_id=channel_id,
                )

        total_count = len(profiles_data)

        logger.info(
            f"Started job with channel ID: {channel_id} for {total_count} resumes."
        )

        # Return the channel_id to the client so it knows where to listen
        return BatchStatusResponse(
            batch_id=batch_id,
            total_count=total_count,
            message=f"Profile processing batch '{batch_id}' dispatched successfully with {total_count} profiles",
            sse_channel=f"{channel_id}",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting pool analysis batch: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.websocket("/ws/stream")
async def websocket_stream(
    websocket: WebSocket,
    batch_id: Optional[str] = None,
    client_id: Optional[str] = None,
):
    """
    Static WebSocket endpoint for streaming results.
    Clients can connect before or after starting a batch.

    Query params:
    - batch_id: The batch to subscribe to (required)
    - client_id: Optional unique client identifier
    """
    global connection_count

    # Validate batch_id
    if not batch_id:
        await websocket.close(code=1008, reason="batch_id query parameter is required")
        return

    # Rate limiting: check max connections
    if connection_count >= MAX_WEBSOCKET_CONNECTIONS:
        await websocket.close(code=1008, reason="Server at maximum connection capacity")
        logger.warning(f"Rejected WebSocket connection - max capacity reached")
        return

    await websocket.accept()
    connection_count += 1

    # Track this connection
    if batch_id not in active_connections:
        active_connections[batch_id] = set()
        
    active_connections[batch_id].add(websocket)

    client_identifier = client_id or f"client_{id(websocket)}"
    logger.info(f"WebSocket connected: {client_identifier} for batch {batch_id}")

    # Send connection acknowledgment
    await websocket.send_json(
        {
            "action": "connected",
            "batch_id": batch_id,
            "client_id": client_identifier,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    )

    r = redis.Redis(connection_pool=redis_pool)
    pubsub = r.pubsub()
    channel_id = f"job_{batch_id}"

    try:
        await pubsub.subscribe(channel_id)
        logger.info(f"Subscribed to channel: {channel_id}")

        # Heartbeat task
        async def send_heartbeat():
            while True:
                try:
                    await asyncio.sleep(30)  # Send heartbeat every 30s
                    await websocket.send_json(
                        {
                            "action": "heartbeat",
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        }
                    )
                except Exception:
                    break

        heartbeat_task = asyncio.create_task(send_heartbeat())

        # Main message loop
        while True:
            try:
                message = await asyncio.wait_for(
                    pubsub.get_message(ignore_subscribe_messages=True), timeout=1.0
                )

                if message and message.get("type") == "message":
                    data = message["data"].decode("utf-8")

                    # Parse and enrich message
                    try:
                        msg_data = json.loads(data)
                        msg_data["received_at"] = datetime.now(timezone.utc).isoformat()
                        await websocket.send_json(msg_data)
                    except json.JSONDecodeError:
                        # Send raw if not JSON
                        await websocket.send_text(data)

            except asyncio.TimeoutError:
                # No message received, check connection health
                try:
                    await websocket.send_json(
                        {"action": "ping", "timestamp": datetime.utcnow().isoformat()}
                    )
                except Exception:
                    logger.warning(f"WebSocket ping failed for {client_identifier}")
                    break
            except WebSocketDisconnect:
                logger.info(f"Client {client_identifier} disconnected normally")
                break
            except Exception as e:
                logger.error(f"Error in WebSocket loop for {client_identifier}: {e}")
                break

    except Exception as e:
        logger.error(f"WebSocket error for batch {batch_id}: {e}", exc_info=True)
    finally:
        # Cleanup
        heartbeat_task.cancel()

        try:
            await pubsub.unsubscribe(channel_id)
            await pubsub.close()
        except Exception as e:
            logger.warning(f"Error during pubsub cleanup: {e}")

        try:
            await r.aclose()
        except Exception as e:
            logger.warning(f"Error closing Redis connection: {e}")

        # Remove from tracking
        if batch_id in active_connections:
            active_connections[batch_id].discard(websocket)
            if not active_connections[batch_id]:
                del active_connections[batch_id]

        connection_count -= 1
        logger.info(
            f"WebSocket closed: {client_identifier} for batch {batch_id}. "
            f"Active connections: {connection_count}"
        )


# @app.websocket("/ws/{batch_id}")
# async def websocket_endpoint(websocket: WebSocket, batch_id: str):
#     """
#     Establishes a WebSocket connection and streams results for a given batch_id.
#     It subscribes to the Redis channel and forwards messages to the client.
#     """
#     await websocket.accept()
#     r = redis.Redis(connection_pool=redis_pool)
#     pubsub = r.pubsub()
#     channel_id = f"job_{batch_id}"
#     await pubsub.subscribe(channel_id)

#     try:
#         # This loop will continuously listen for new messages from Redis
#         while True:
#             message = await pubsub.get_message(
#                 ignore_subscribe_messages=True, timeout=1.0
#             )
#             if message and message.get("type") == "message":
#                 # Forward the message to the client over the WebSocket
#                 await websocket.send_text(message["data"].decode("utf-8"))
#     except WebSocketDisconnect:
#         logger.info(f"Client disconnected from WebSocket for batch {batch_id}")
#     except Exception as e:
#         logger.error(f"Error in WebSocket for batch {batch_id}: {e}")
#     finally:
#         # Clean up the connection and subscription when the client disconnects
#         logger.info(f"Unsubscribing and closing connection for batch {batch_id}")
#         await pubsub.unsubscribe(channel_id)
#         await r.aclose()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, log_level="info", access_log=True)
