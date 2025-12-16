# app/core/auth.py
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from app.models.db import User
from tortoise.exceptions import DoesNotExist
import httpx
from httpx import AsyncClient

bearer_auth = HTTPBearer(auto_error=False)


JWKS_URL = "https://workable-kit-45.clerk.accounts.dev/.well-known/jwks.json" 


async def get_jwks_data():
    async with httpx.AsyncClient() as client:
        response = await client.get(JWKS_URL)
        response.raise_for_status()
        return response.json()
async def get_current_user(credentials: HTTPAuthorizationCredentials | None = Depends(bearer_auth)):
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization header",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = credentials.credentials

    try:
        # Get JWKS and find the right key
        jwks_data = await get_jwks_data()
        unverified_header = jwt.get_unverified_header(token)
        rsa_key = {}
        for key in jwks_data["keys"]:
            if key["kid"] == unverified_header["kid"]:
                rsa_key = {
                    "kty": key["kty"],
                    "kid": key["kid"],
                    "use": key["use"],
                    "n": key["n"],
                    "e": key["e"],
                }
                break

        if not rsa_key:
            raise JWTError("Public key not found")

        # Verify and decode the token
        payload = jwt.decode(
            token,
            rsa_key,
            algorithms=["RS256"],
            options={"verify_aud": False},  # Set to True and add audience if you use custom JWT templates
        )

        clerk_user_id: str = payload["sub"]
        print(f"Authenticated Clerk user: {clerk_user_id}")

    except JWTError as e:
        print(f"JWT verification failed: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired Clerk token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except Exception as e:
        print(f"Unexpected auth error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication failed",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Create or get local user
    try:
        user = await User.get(clerk_id=clerk_user_id)
    except DoesNotExist:
        user = await User.create(clerk_id=clerk_user_id)
        print(f"New local user created for Clerk ID: {clerk_user_id}")

    return user