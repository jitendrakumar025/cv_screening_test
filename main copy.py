# import asyncio
# import redis.asyncio as redis
# from fastapi import FastAPI, Request
# from fastapi.middleware.cors import CORSMiddleware
# from sse_starlette.sse import EventSourceResponse
# import os
# from dotenv import load_dotenv
# import logging
# from pydantic import BaseModel
# import socket


# from tasks import struct_resume_task, analyze_resume_task, pool_analysis_task

# app = FastAPI()

# load_dotenv()
# REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)


# redis_pool = redis.ConnectionPool.from_url(
#     REDIS_URL,
#     max_connections=50,
# )

# # --- CORS Middleware ---
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )


# class BatchStatusResponse(BaseModel):
#     batch_id: str
#     total_count: int
#     message: str
#     sse_channel: str


# @app.get("/")
# async def root():
#     return {"message": "Welcome to the resume processing API!"}


# @app.get("/health")
# async def check_health():
#     return {"status": "Okk"}


# @app.post("/start-resume-structuring")
# async def start_resume_structuring(request: Request):
#     """
#     Accepts a list of resumes, creates a unique channel ID,
#     and dispatches a Celery task for each resume.
#     """
#     try:
#         data = await request.json()
#         resumes_data = data.get("resume_list")
#         batch_id = data.get("batch_id")
#         logger.info(f"Received resume list for batch_id:{batch_id}")
#         if resumes_data is None or batch_id is None:
#             logger.error("Either Resume List or Batch Id is None!")
#             return ""

#         if not isinstance(resumes_data, list) or not all(
#             "resume_id" in r and "resume_text" in r for r in resumes_data
#         ):
#             return {
#                 "error": "Request must be a JSON list of objects, each with 'resume_id' and 'resume_text'."
#             }

#         # Unique ID for this specific request, used as the Redis Pub/Sub channel
#         channel_id = f"job_{batch_id}"

#         # Dispatch a task for each resume, telling it which channel to publish to
#         for resume in resumes_data:
#             resume_id = resume.get("resume_id")
#             resume_text = resume.get("resume_text")
#             if resume_id and resume_text:
#                 struct_resume_task.delay(resume_id, resume_text, channel_id, batch_id)

#         total_count = len(resumes_data)

#         logger.info(
#             f"Started job with channel ID: {channel_id} for {total_count} resumes."
#         )

#         # Return the channel_id to the client so it knows where to listen
#         return BatchStatusResponse(
#             batch_id=batch_id,
#             total_count=total_count,
#             message=f"Resume processing batch '{batch_id}' dispatched successfully with {total_count} resumes",
#             sse_channel=f"{channel_id}",
#         )
#     except Exception as e:
#         logger.error(e)


# @app.post("/start-resume-analysis")
# async def start_resume_analysis(request: Request):
#     """
#     Accepts a list of resumes, creates a unique channel ID,
#     and dispatches a Celery task for each resume.
#     """
#     data = await request.json()
#     resumes_data = data.get("resume_list")
#     batch_id = data.get("batch_id")
#     parameters = data.get("parameters")

#     if resumes_data is None or batch_id is None or parameters is None:
#         logger.error(
#             "Either Resume List or Batch Id,parameters is None! Internal Server Error!"
#         )
#         return ""

#     if not isinstance(resumes_data, list) or not all(
#         "resume_id" in r and "resume_text" in r for r in resumes_data
#     ):
#         return {
#             "error": "Request must be a JSON list of objects, each with 'resume_id' and 'resume_text'."
#         }

#     # Unique ID for this specific request, used as the Redis Pub/Sub channel
#     channel_id = f"job_{batch_id}"

#     # Dispatch a task for each resume, telling it which channel to publish to
#     for resume in resumes_data:
#         resume_id = resume.get("resume_id")
#         resume_text = resume.get("resume_text")
#         candidate_id = resume.get("candidate_id")
#         round_id = resume.get("round_id")
#         if resume_id and resume_text:
#             analyze_resume_task.delay(
#                 resume_id,
#                 resume_text,
#                 channel_id,
#                 batch_id,
#                 candidate_id,
#                 round_id,
#                 parameters,
#             )

#     total_count = len(resumes_data)

#     logger.info(f"Started job with channel ID: {channel_id} for {total_count} resumes.")

#     # Return the channel_id to the client so it knows where to listen
#     return BatchStatusResponse(
#         batch_id=batch_id,
#         total_count=total_count,
#         message=f"Resume processing batch '{batch_id}' dispatched successfully with {total_count} resumes",
#         sse_channel=f"{channel_id}",
#     )


# @app.post("/start-talentPool-analysis")
# async def start_talentPool_analysis(request: Request):
#     """
#     Accepts a list of resumes, creates a unique channel ID,
#     and dispatches a Celery task for each resume.
#     """
#     data = await request.json()
#     profiles_data = data.get("profile_analysis_list")
#     batch_id = data.get("batch_id")
#     parameters = data.get("parameters")

#     if profiles_data is None or batch_id is None or parameters is None:
#         logger.error(
#             "Either Profiles List or Batch Id,parameters is None! Internal Server Error!"
#         )
#         return ""

#     # Unique ID for this specific request, used as the Redis Pub/Sub channel
#     channel_id = f"job_{batch_id}"

#     # Dispatch a task for each resume, telling it which channel to publish to
#     for profile in profiles_data:
#         profile_id = profile.pool_profile.get("id")
#         profile_analysis = profile.get("analysis")
#         if profile_id and profile_analysis:
#             pool_analysis_task.delay(
#                 candidate_profile=profile_analysis,
#                 parameters=parameters,
#                 profile_id=profile_id,
#                 batch_id=batch_id,
#                 channel_id=channel_id,
#             )

#     total_count = len(profiles_data)

#     logger.info(f"Started job with channel ID: {channel_id} for {total_count} resumes.")

#     # Return the channel_id to the client so it knows where to listen
#     return BatchStatusResponse(
#         batch_id=batch_id,
#         total_count=total_count,
#         message=f"Profile processing batch '{batch_id}' dispatched successfully with {total_count} profiles",
#         sse_channel=f"{channel_id}",
#     )


# @app.get("/stream/{channel_type}/{batch_id}")
# async def stream_results(batch_id: str):
#     """
#     This endpoint uses Server-Sent Events to stream results.
#     It subscribes to the Redis channel and forwards messages.
#     Valid channel_types: 'resume-status', 'pool-status'

#     """
#     try:

#         async def event_generator():
#             # Connect to Redis Pub/Sub using the Azure URL
#             # r = await redis.from_url(REDIS_URL)
#             r = redis.Redis(
#                 connection_pool=redis_pool,
#                 socket_keepalive=True,
#                 socket_keepalive_options={
#                     socket.TCP_KEEPIDLE: 1,  # Use socket constants, not strings
#                     socket.TCP_KEEPINTVL: 3,  # These are the actual integer constants
#                     socket.TCP_KEEPCNT: 5,
#                 },
#                 health_check_interval=30,
#             )
#             pubsub = r.pubsub()
#             channel_id = f"job_{batch_id}"
#             await pubsub.subscribe(channel_id)

#             try:
#                 while True:
#                     message = await pubsub.get_message(
#                         ignore_subscribe_messages=True, timeout=1.0
#                     )
#                     if message:
#                         yield message["data"].decode("utf-8")
#                     await asyncio.sleep(0.01)
#             except asyncio.CancelledError:
#                 logger.info(f"Client disconnected from channel {channel_id}")
#             finally:
#                 logger.info(f"Finally! Client disconnected from channel {channel_id}")
#                 await pubsub.unsubscribe(channel_id)
#                 await r.close()

#         return EventSourceResponse(
#             event_generator(),
#             media_type="text/event-stream",
#             headers={
#                 "Cache-Control": "no-cache",
#                 "Connection": "keep-alive",
#                 "X-Accel-Buffering": "no",  # Critical for Azure
#                 "Transfer-Encoding": "chunked",
#             },
#         )

#     except Exception as e:
#         logger.error(e)


# if __name__ == "__main__":
#     import uvicorn

#     uvicorn.run(app)
