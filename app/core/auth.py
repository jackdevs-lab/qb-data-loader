# app/core/auth.py

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from app.models.db import User
from tortoise.exceptions import DoesNotExist
import httpx

bearer_auth = HTTPBearer(auto_error=False)

JWKS_URL = "https://workable-kit-45.clerk.accounts.dev/.well-known/jwks.json"

# Cache JWKS to avoid fetching on every request (optional but recommended)
_jwks_cache = None
async def get_jwks_data():
    global _jwks_cache
    if _jwks_cache is None:
        async with httpx.AsyncClient() as client:
            response = await client.get(JWKS_URL)
            response.raise_for_status()
            _jwks_cache = response.json()
    return _jwks_cache

async def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_auth)
) -> User:
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization header",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = credentials.credentials

    try:
        jwks_data = await get_jwks_data()
        unverified_header = jwt.get_unverified_header(token)
        rsa_key = next(
            (key for key in jwks_data["keys"] if key["kid"] == unverified_header["kid"]),
            None,
        )
        if not rsa_key:
            raise JWTError("Public key not found")

        payload = jwt.decode(
            token,
            rsa_key,
            algorithms=["RS256"],
            options={"verify_aud": False},  # Enable and set audience in production if needed
        )

        clerk_user_id: str = payload.get("sub")
        if not clerk_user_id:
            raise JWTError("Missing sub in token")

        print(f"Authenticated Clerk user: {clerk_user_id}")  # Keep this — great for debugging

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
        )

    # Get or create local user
    try:
        user = await User.get(clerk_id=clerk_user_id)
    except DoesNotExist:
        user = await User.create(clerk_id=clerk_user_id)
        print(f"New local user created for Clerk ID: {clerk_user_id}")

    return user