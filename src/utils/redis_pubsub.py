import redis
import os
from redis.connection import ConnectionPool
import json,ssl

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

use_ssl = REDIS_URL.startswith("rediss://")

# Create connection pool for better performance
connection_pool = ConnectionPool.from_url(
    REDIS_URL,
    decode_responses=True,
    max_connections=20,
    retry_on_timeout=True,
    socket_keepalive=True,
    ssl_cert_reqs='required' if use_ssl else None,
    ssl_ca_certs="/etc/ssl/certs/ca-certificates.crt"
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
    pubsub = r.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe(channel)
    return pubsub

def increment_batch_progress(batch_id, total_count, redis_client):
    """
    Atomically increment batch progress and check for completion
    Returns: (current_count, is_batch_complete)
    """
    progress_key = f"batch_progress:{batch_id}"
    completion_key = f"batch_completed:{batch_id}"
    
    # Simpler approach: Use Redis INCR with pipeline for atomicity
    try:
        pipe = redis_client.pipeline()
        pipe.incr(progress_key)
        pipe.expire(progress_key, 3600)
        pipe.get(completion_key)
        results = pipe.execute()
        
        current_count = results[0]
        already_completed = results[2]
        
        print(f"📊 Batch {batch_id}: {current_count}/{total_count} completed")
        
        # Check if batch is complete and not already marked as completed
        if current_count >= total_count and not already_completed:
            # Mark as completed to prevent duplicates
            redis_client.setex(completion_key, 3600, '1')
            
            # Publish completion message
            status_channel = f"resume_status_{batch_id}"
            completion_msg = {
                "action": "batch_complete",
                "batch_id": batch_id,
                "total_count": total_count,
                "completed_count": current_count,
                "message": f"Batch {batch_id} completed! All {total_count} resumes processed"
            }
            
            publish_message(status_channel, json.dumps(completion_msg))
            print(f"🎉 Batch {batch_id} completed! All {total_count} resumes processed")
            
            return current_count, True
        
        return current_count, current_count >= total_count
        
    except Exception as e:
        print(f"Error in increment_batch_progress: {e}")
        # Fallback to simple increment
        current_count = redis_client.incr(progress_key)
        redis_client.expire(progress_key, 3600)
        return current_count, current_count >= total_count

def get_batch_progress(batch_id):
    """Get processing progress for a batch"""
    try:
        progress_key = f"batch_progress:{batch_id}"
        count = r.get(progress_key)
        return int(count) if count else 0
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
        
        # Clean up batch progress
        progress_key = f"batch_progress:{batch_id}"
        completion_key = f"batch_completed:{batch_id}"
        r.delete(progress_key, completion_key)
        
        # Optionally clean up old results
        # result_pattern = f"result:*batch_{batch_id}*"
        # result_keys = r.keys(result_pattern)
        # if result_keys:
        #     r.delete(*result_keys)
        
    except Exception as e:
        print(f"Error cleaning up batch data: {e}")