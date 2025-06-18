import redis
import os
from redis.connection import ConnectionPool

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

# Create connection pool for better performance
connection_pool = ConnectionPool.from_url(
    REDIS_URL,
    decode_responses=True,
    max_connections=20,  # Adjust based on your needs
    retry_on_timeout=True,
    socket_keepalive=True,
    socket_keepalive_options={}
)

# Redis connection with connection pooling
r = redis.Redis(connection_pool=connection_pool)

def get_redis_client():
    """Get Redis client instance"""
    return r

def publish_message(channel, message):
    """Publish message to Redis channel"""
    try:
        r.publish(channel, message)
    except Exception as e:
        print(f"Error publishing message: {e}")

def subscribe_to_channel(channel):
    """Subscribe to Redis channel"""
    pubsub = r.pubsub()
    pubsub.subscribe(channel)
    return pubsub

def get_batch_progress(batch_id):
    """Get processing progress for a batch"""
    try:
        # Count completed tasks
        pattern = f"result:*batch_{batch_id}*"
        completed_keys = r.keys(pattern)
        return len(completed_keys)
    except Exception as e:
        print(f"Error getting batch progress: {e}")
        return 0

def cleanup_batch_data(batch_id, max_age_seconds=3600):
    """Clean up old batch data"""
    try:
        # Clean up processing locks
        processing_pattern = f"processing:*batch_{batch_id}*"
        processing_keys = r.keys(processing_pattern)
        if processing_keys:
            r.delete(*processing_keys)
        
        # Optionally clean up old results
        # result_pattern = f"result:*batch_{batch_id}*"
        # result_keys = r.keys(result_pattern)
        # if result_keys:
        #     r.delete(*result_keys)
        
    except Exception as e:
        print(f"Error cleaning up batch data: {e}")