import os
from celery import Celery

redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")

celery_app = Celery(
    "resume_tasks",
    broker=redis_url,
    backend=redis_url,
    include=["src.workers.resume_tasks"],
) 

# Optimized configuration for high-volume processing
celery_app.conf.update(
    # Serialization
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    
    # Task routing and execution
    task_routes={
        'src.workers.resume_tasks.process_resume_task': {'queue': 'resume_analysis'},
        'src.workers.resume_tasks.struct_resume_task': {'queue': 'resume_structuring'},
    },
    
    # Performance optimizations
    worker_prefetch_multiplier=1,  # Prevent worker hoarding tasks
    task_acks_late=True,  # Acknowledge tasks only after completion
    worker_disable_rate_limits=True,  # Remove rate limiting for max throughput
    
    # Retry configuration
    task_default_retry_delay=60,  # 1 minute retry delay
    task_max_retries=3,
    
    # Result backend settings
    result_expires=3600,  # Results expire after 1 hour
    result_persistent=True,  # Persist results to Redis
    
    # Memory optimization
    worker_max_tasks_per_child=100,  # Restart worker after 100 tasks to prevent memory leaks
    
    # Concurrency settings
    worker_concurrency=10,  # Match your desired concurrency level
)