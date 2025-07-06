import os
from celery import Celery
from kombu import Queue
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
    },
    
    # Connection pool settings - CRITICAL for Redis Cloud
    broker_connection_retry_on_startup=True,
    broker_connection_retry=True,
    broker_connection_max_retries=10,
    
    # Redis backend settings
    redis_max_connections=8,  # Limit connections per worker
    redis_retry_on_timeout=True,
    redis_socket_keepalive=True,
    redis_socket_keepalive_options={},
    
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
    worker_max_tasks_per_child=50,  # Restart worker after 50 tasks to prevent memory leaks
    
    # Concurrency settings - REDUCED for connection limits
    worker_concurrency=4,  # Reduced from 10 to 4
    
    # Queue settings
    task_queues=[
        Queue('resume_analysis', routing_key='resume_analysis'),
    ],
    
    # Connection pooling
    broker_pool_limit=3,  # Limit broker connection pool
    broker_connection_timeout=10,
    broker_transport_options={
        'max_connections': 8,
        'retry_on_timeout': True,
        'socket_keepalive': True,
        'socket_keepalive_options': {},
        'socket_connect_timeout': 10,
        'socket_timeout': 10,
        'health_check_interval': 30,
    },
    
    # Result backend connection settings
    result_backend_transport_options={
        'max_connections': 8,
        'retry_on_timeout': True,
        'socket_keepalive': True,
        'socket_keepalive_options': {},
        'socket_connect_timeout': 10,
        'socket_timeout': 10,
        'health_check_interval': 30,
    },
    
    # Reduce connection usage
    worker_send_task_events=False,  # Disable task events to reduce connections
    task_send_sent_event=False,  # Disable sent events
    
    # Timezone
    timezone='UTC',
    enable_utc=True,
)



def cleanup_connections():
    """Clean up Redis connections on worker shutdown"""
    try:
        celery_app.control.pool_restart()
        print("Celery connections cleaned up")
    except Exception as e:
        print(f"Error cleaning up Celery connections: {e}")

# Register cleanup function
import atexit
atexit.register(cleanup_connections)