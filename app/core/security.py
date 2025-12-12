# app/core/security.py
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer

# Temporary dummy auth – everyone is logged in as user_id=1
security = HTTPBearer(auto_error=False)

async def get_current_user(token=Depends(security)):
    # Remove this later when you add real auth
    class DummyUser:
        id = 1
    return DummyUser()