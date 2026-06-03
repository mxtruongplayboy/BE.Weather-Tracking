from fastapi import APIRouter, WebSocket, WebSocketDisconnect
import json
import asyncio

router = APIRouter(prefix="/api/v1/lightning", tags=["lightning"])

# This is a placeholder for real Redis connections
# We will use a mock active connection list for now
active_connections = []

@router.get("/recent")
def get_recent_lightning():
    # TODO: Fetch from Redis
    return {"status": "ok", "data": []}

@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.append(websocket)
    try:
        while True:
            # We don't expect client to send much, mostly just keep-alive
            data = await websocket.receive_text()
    except WebSocketDisconnect:
        active_connections.remove(websocket)

async def broadcast_lightning(strike_data: dict):
    # This will be called by the crawler worker when a new strike is received
    message = json.dumps(strike_data)
    for connection in active_connections:
        try:
            await connection.send_text(message)
        except Exception:
            pass
