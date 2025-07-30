# src/utils/async_redis.py

import redis.asyncio as redis
import os
from redis.asyncio.connection import ConnectionPool
import logging

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
MAX_REDIS_CONNECTIONS = int(os.getenv("MAX_REDIS_CONNECTIONS", "100"))

use_ssl = REDIS_URL.startswith("rediss://")

# Asynchronous connection pool
try:
    async_connection_pool = ConnectionPool.from_url(
        REDIS_URL,
        decode_responses=True,
        max_connections=MAX_REDIS_CONNECTIONS,
        socket_keepalive=True,
        ssl_cert_reqs='required' if use_ssl else None,
        ssl_ca_certs="/etc/ssl/certs/ca-certificates.crt"
    )

    # Asynchronous Redis client instance
    async_redis_client = redis.Redis(connection_pool=async_connection_pool)
    logger.info("✅ Asynchronous Redis connection pool created successfully.")

except Exception as e:
    logger.error(f"❌ Failed to create asynchronous Redis connection pool: {e}")
    async_redis_client = None

def get_async_redis_client():
    """Get the shared asynchronous Redis client instance."""
    if async_redis_client is None:
        raise ConnectionError("Asynchronous Redis client is not available.")
    return async_redis_client

async def redis_health_check_async():
    """Check async Redis connection health."""
    if async_redis_client is None:
        return False
    try:
        await async_redis_client.ping()
        return True
    except Exception as e:
        logger.error(f"Async Redis health check failed: {e}")
        return False
