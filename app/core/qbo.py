# app/core/qbo.py
import httpx
import time
from app.models.db import User
from app.core.config import settings

BASE_URL = "https://sandbox-quickbooks.api.intuit.com/v3/company"  # keep trailing /company

async def refresh_token_if_needed(user: User):
    # Refresh if missing, expired, or expires in next 5 minutes
    if not user.qbo_access_token or not user.qbo_expires_at:
        expires_at = 0
    else:
        expires_at = user.qbo_expires_at

    if expires_at < time.time() + 300:  # 5-minute buffer
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": user.qbo_refresh_token,
                },
                auth=(settings.QBO_CLIENT_ID, settings.QBO_CLIENT_SECRET),
            )
            resp.raise_for_status()
            data = resp.json()

            user.qbo_access_token = data["access_token"]
            user.qbo_expires_at = int(time.time()) + data["expires_in"] - 60  # safety margin
            if "refresh_token" in data:  # sometimes rotated
                user.qbo_refresh_token = data["refresh_token"]
            await user.save()

async def get_qbo_client(user: User) -> httpx.AsyncClient:
    await refresh_token_if_needed(user)

    headers = {
        "Authorization": f"Bearer {user.qbo_access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    client = httpx.AsyncClient(
        base_url=f"{BASE_URL}/{user.qbo_realm_id}",
        headers=headers,
        timeout=30.0,
    )
    return client