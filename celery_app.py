import os
from celery import Celery
from dotenv import load_dotenv
import socket

# Load environment variables from .env file
load_dotenv()

# Get the Redis URL from the environment variable
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

celery = Celery("tasks", broker=REDIS_URL, backend=REDIS_URL, include=["tasks"])

celery.conf.update(
    task_track_started=True,
    timezone="UTC",
    enable_utc=True,
    # Socket settings
    broker_transport_options={
        "socket_keepalive": True,
        "socket_keepalive_options": {
            socket.TCP_KEEPIDLE: 1,  # Use socket constants, not strings
            socket.TCP_KEEPINTVL: 3,  # These are the actual integer constants
            socket.TCP_KEEPCNT: 5,
        },
        "health_check_interval": 30,
    },
    result_backend_transport_options={
        "socket_keepalive": True,
        "socket_keepalive_options": {
            socket.TCP_KEEPIDLE: 1,  # Use socket constants, not strings
            socket.TCP_KEEPINTVL: 3,  # These are the actual integer constants
            socket.TCP_KEEPCNT: 5,
        },
        "health_check_interval": 30,
    },
    broker_use_ssl={"ssl_cert_reqs": "none"},
    redis_backend_use_ssl={"ssl_cert_reqs": "none"},
    # broker_use_ssl={'ssl_cert_reqs': 'required', 'ssl_ca_certs': "/etc/ssl/certs/ca-certificates.crt"},
    # redis_backend_use_ssl={'ssl_cert_reqs': 'required', 'ssl_ca_certs': "/etc/ssl/certs/ca-certificates.crt"},
    redis_socket_keepalive=True,
    redis_socket_connect_timeout=10,
    redis_retry_on_timeout=True,
    redis_max_connections=int(os.getenv("MAX_REDIS_CONN", "100"))
    # redis_max_connections=100,
)
