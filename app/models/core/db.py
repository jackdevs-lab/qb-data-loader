# app/core/db.py
import ssl
from app.core.config import settings

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

TORTOISE_ORM = {
    "connections": {
        "default": {
            "engine": "tortoise.backends.asyncpg",
            "credentials": {
                "host": "ep-curly-forest-a431wpea-pooler.us-east-1.aws.neon.tech",
                "port": 5432,
                "user": "neondb_owner",
                "password": "npg_pW2ae5rDmhGY",
                "database": "neondb",
                "ssl": ssl_context,
            }
        }
    },
    "apps": {
        "models": {
            "models": ["app.models.db", "aerich.models"],  # you can even remove "aerich.models" now
            "default_connection": "default",
        }
    },
    "use_tz": False,
    "timezone": "UTC",
}