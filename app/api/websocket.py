# app/api/websocket.py   ← new file
from fastapi import APIRouter, WebSocket, Depends
from app.core.websocket import manager
from app.models.db import Job
from app.core.auth import get_current_user

router = APIRouter()

@router.websocket("/ws/job/{job_id}")
async def websocket_endpoint(websocket: WebSocket, job_id: int):
    await manager.connect(websocket, job_id)
    try:
        while True:
            # Keep alive + allow client to send pings
            data = await websocket.receive_json()
            # ignore client messages for now
    except:
        manager.disconnect(websocket, job_id)