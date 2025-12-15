# app/core/websocket.py
from fastapi import WebSocket
from typing import Dict, Set
import json

class ConnectionManager:
    def __init__(self):
        # job_id → set of active WebSocket connections
        self.active_connections: Dict[int, Set[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, job_id: int):
        await websocket.accept()
        if job_id not in self.active_connections:
            self.active_connections[job_id] = set()
        self.active_connections[job_id].add(websocket)

    def disconnect(self, websocket: WebSocket, job_id: int):
        if job_id in self.active_connections:
            self.active_connections[job_id].discard(websocket)
            if not self.active_connections[job_id]:
                del self.active_connections[job_id]

    async def broadcast(self, message: dict, job_id: int):
        if job_id not in self.active_connections:
            return
        dead_connections = set()
        for connection in self.active_connections[job_id]:
            try:
                await connection.send_json(message)
            except Exception:
                dead_connections.add(connection)
        # Clean up dead connections
        for conn in dead_connections:
            self.active_connections[job_id].discard(conn)

# Create a single global instance
manager = ConnectionManager()