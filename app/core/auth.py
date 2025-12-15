# app/core/auth.py
import httpx
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from clerk_backend_api import Clerk
from clerk_backend_api.security.types import AuthenticateRequestOptions
from app.models.db import User
from tortoise.exceptions import DoesNotExist
from app.core.config import settings

clerk_sdk = Clerk(bearer_auth=settings.CLERK_SECRET_KEY)

bearer_auth = HTTPBearer(auto_error=False)

async def get_current_user(request: Request, credentials: HTTPAuthorizationCredentials | None = Depends(bearer_auth)):
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization header",
            headers={"WWW-Authenticate": "Bearer"},
        )

    httpx_request = httpx.Request(
        method=request.method,
        url=str(request.url),
        headers=request.headers,
    )

    try:
        # REQUIRED: Pass an empty AuthenticateRequestOptions() — this works for standard session tokens
        request_state = clerk_sdk.authenticate_request(
            httpx_request,
            AuthenticateRequestOptions()  # Empty options = default session token verification (no azp check)
        )

        if not request_state.is_signed_in:
            print(f"Clerk auth failed: {request_state.reason}")  # Debug in console
            raise ValueError(request_state.reason or "Invalid session")

        clerk_user_id = request_state.payload["sub"]
        print(f"Authenticated Clerk user: {clerk_user_id}")  # Confirmation in console
    except Exception as e:
        print(f"Clerk verification error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid Clerk session: {str(e)}",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        user = await User.get(clerk_id=clerk_user_id)
    except DoesNotExist:
        user = await User.create(clerk_id=clerk_user_id)
        print(f"New local user created for Clerk ID: {clerk_user_id}")

    return user