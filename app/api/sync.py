# New File: app/api/sync.py (For Real-Time Sync)
from fastapi import APIRouter, Depends, HTTPException, Body
from app.core.auth import get_current_user
from app.models.db import User
from app.core.qbo import get_qbo_client
import httpx
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/sync", tags=["sync"])

@router.post("/webhook")
async def handle_webhook(payload: dict):
    # Verify signature (production must)
    # Process CDC events
    logger.info(f"Webhook received: {payload}")
    # Update local DB or trigger jobs
    return {"status": "received"}

@router.get("/status")
async def get_sync_status(current_user: User = Depends(get_current_user)):
    # Check last sync time from DB
    return {"last_sync": "2026-01-19T12:00:00", "status": "up_to_date"}

@router.post("/manual")
async def manual_sync(entity: str = Body(...), current_user: User = Depends(get_current_user)):
    client = await get_qbo_client(current_user)
    try:
        if entity == "customers":
            query = "SELECT * FROM Customer WHERE MetaData.LastUpdatedTime > 'last_sync_time'"
            resp = await client.get("/query", params={"query": query, "minorversion": "75"})
            resp.raise_for_status()
            # Process and store
            return {"synced": len(resp.json().get("QueryResponse", {}).get("Customer", []))}
        # Add for other entities
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        await client.aclose()
