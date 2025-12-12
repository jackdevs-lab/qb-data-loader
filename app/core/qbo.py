# app/core/qbo.py
import httpx
import time
from app.models.db import User
from app.core.config import settings

BASE_URL = "https://sandbox-quickbooks.api.intuit.com/v3/company"

async def get_qbo_client(user: User) -> httpx.AsyncClient:
    # Auto-refresh if token missing or expires in < 5 min
    if not user.qbo_access_token or (user.qbo_expires_at or 0) < time.time() + 300:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
                data={"grant_type": "refresh_token", "refresh_token": user.qbo_refresh_token},
                auth=(settings.QBO_CLIENT_ID, settings.QBO_CLIENT_SECRET),
            )
            resp.raise_for_status()
            data = resp.json()
            user.qbo_access_token = data["access_token"]
            user.qbo_refresh_token = data["refresh_token"]
            user.qbo_expires_at = int(time.time()) + data["expires_in"]
            await user.save()

    headers = {"Authorization": f"Bearer {user.qbo_access_token}", "Accept": "application/json"}
    return httpx.AsyncClient(headers=headers, base_url=f"{BASE_URL}/{user.qbo_realm_id}/")