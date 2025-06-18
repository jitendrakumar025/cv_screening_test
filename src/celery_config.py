import os
from celery import Celery

redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")


celery_app = Celery(
    "resume_tasks",
    broker=redis_url,
    backend=redis_url,
    include=["src.workers.resume_tasks"],
) 


celery_app.conf.update(
    task_serializer='json',
    result_serializer='json',
    accept_content=['json']
)
