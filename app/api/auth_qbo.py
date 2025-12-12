# app/api/auth_qbo.py
import time
import httpx
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import RedirectResponse
from urllib.parse import urlencode
from app.models.db import User
from app.core.config import settings

router = APIRouter()
@router.get("/qbo/login")
async def login():
    params = {
        "client_id": settings.QBO_CLIENT_ID,
        "redirect_uri": settings.QBO_REDIRECT_URI,
        "response_type": "code",
        "scope": "com.intuit.quickbooks.accounting",
        "state": "xyz123",
    }
    url = f"https://appcenter.intuit.com/connect/oauth2?{urlencode(params)}"
    return RedirectResponse(url)

@router.get("/callback")
async def callback(code: str, realmId: str):
    user = await User.get_or_none(id=1)
    if not user:
        user = await User.create(id=1)

    async with httpx.AsyncClient() as client:
        r = await client.post(
            "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": settings.QBO_REDIRECT_URI,
            },
            auth=(settings.QBO_CLIENT_ID, settings.QBO_CLIENT_SECRET),
        )
        r.raise_for_status()
        data = r.json()

    user.qbo_realm_id = realmId
    user.qbo_access_token = data["access_token"]
    user.qbo_refresh_token = data["refresh_token"]
    user.qbo_expires_at = int(__import__("time").time()) + data["expires_in"]
    await user.save()

    return {"message": "QuickBooks connected!", "company_id": realmId}