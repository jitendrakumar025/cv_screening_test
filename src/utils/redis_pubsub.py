import redis

import os

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

# Redis connection
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

def publish_message(channel, message):
    r.publish(channel, message)

def subscribe_to_channel(channel):
    pubsub = r.pubsub()
    pubsub.subscribe(channel)
    return pubsub
