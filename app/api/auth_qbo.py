import uuid
import time
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import RedirectResponse
from fastapi.responses import HTMLResponse
from urllib.parse import urlencode
from app.core.auth import get_current_user  # Your existing Clerk dependency
from app.models.db import User
from app.core.config import settings
from app.core.redis import redis_client  # New import
import httpx

router = APIRouter()

STATE_TTL = 600  # 10 minutes

@router.get("/qbo/login")
async def qbo_login(current_user: User = Depends(get_current_user)):
    state = str(uuid.uuid4())
    await redis_client.setex(f"qbo_state:{state}", STATE_TTL, current_user.clerk_id)

    params = {
        "client_id": settings.QBO_CLIENT_ID,
        "redirect_uri": settings.QBO_REDIRECT_URI,
        "response_type": "code",
        "scope": "com.intuit.quickbooks.accounting",
        "state": state,
    }
    intuit_url = f"https://appcenter.intuit.com/connect/oauth2?{urlencode(params)}"

    # Return JSON instead of redirect
    return {"redirect_url": intuit_url}
@router.get("/callback")
async def callback(
    code: str = Query(...),
    state: str = Query(None),
    realmId: str = Query(None),
):
    # Public endpoint — no auth required

    if not code:
        raise HTTPException(status_code=400, detail="Missing authorization code")
    if not state:
        raise HTTPException(status_code=400, detail="Missing state parameter")
    if not realmId:
        raise HTTPException(status_code=400, detail="Missing realmId")

    # Lookup and validate state
    clerk_id = await redis_client.get(f"qbo_state:{state}")
    if not clerk_id:
        raise HTTPException(status_code=400, detail="Invalid or expired state")

    # Delete the state immediately (one-time use)
    await redis_client.delete(f"qbo_state:{state}")

    # Fetch the user by clerk_id (adjust query if your User model uses different field)
    user = await User.get(clerk_id=clerk_id)
    if not user:
        raise HTTPException(status_code=400, detail="User not found")

    # Exchange code for tokens
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": settings.QBO_REDIRECT_URI,
            },
            auth=(settings.QBO_CLIENT_ID, settings.QBO_CLIENT_SECRET),
            headers={"Accept": "application/json"},
        )
        if resp.status_code != 200:
            raise HTTPException(status_code=400, detail=f"Token exchange failed: {resp.text}")
        data = resp.json()

    # Save tokens to the correct user
    user.qbo_realm_id = realmId
    user.qbo_access_token = data["access_token"]
    user.qbo_refresh_token = data["refresh_token"]
    user.qbo_expires_at = int(time.time()) + data["expires_in"]
    await user.save()

    # Redirect to frontend (you can add ?success=true or handle in frontend JS)
    return HTMLResponse("""
        <html>
        <body>
            <h2>QuickBooks connected successfully!</h2>
            <p>You can close this window.</p>
            <script>
            // Tell the parent window (if it exists) that we're done
            if (window.opener) {
                window.opener.postMessage('qbo_connected', '*');
                window.close();
            }
            </script>
        </body>
        </html>
        """)