import os
from celery import Celery
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the Redis URL from the environment variable
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

celery = Celery(
    'tasks',
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=['tasks']
)

celery.conf.update(
    task_track_started=True,
    timezone='UTC',
    enable_utc=True,
    broker_transport_options={
        'socket_keepalive': True,
    },
    result_backend_transport_options={
        'socket_keepalive': True,
    },
    broker_use_ssl={'ssl_cert_reqs': 'required', 'ssl_ca_certs': "/etc/ssl/certs/ca-certificates.crt"},
    redis_backend_use_ssl={'ssl_cert_reqs': 'required', 'ssl_ca_certs': "/etc/ssl/certs/ca-certificates.crt"},
)