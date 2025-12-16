import redis.asyncio as redis
from app.core.config import settings

redis_client = redis.from_url(settings.CELERY_BROKER_URL, encoding="utf-8", decode_responses=True)