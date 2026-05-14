from fastapi import WebSocket
from typing import Dict, List, Set
import logging
import json

logger = logging.getLogger("uvicorn.info")

class ConnectionManager:
    """Manages websocket connections and topic subscriptions"""
    def __init__(self):
        self.subscriptions: Dict[str, List[WebSocket]] = {}
        self.socket_subscriptions: Dict[WebSocket, Set[str]] = {}

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.socket_subscriptions[websocket] = set()

    def disconnect(self, websocket: WebSocket):
        if websocket in self.socket_subscriptions:
            for topic in self.socket_subscriptions[websocket]:
                if topic in self.subscriptions and websocket in self.subscriptions[topic]:
                    self.subscriptions[topic].remove(websocket)
            del self.socket_subscriptions[websocket]

    async def subscribe(self, websocket: WebSocket, topic: str):
        if topic not in self.subscriptions:
            self.subscriptions[topic] = []
        if websocket not in self.subscriptions[topic]:
            self.subscriptions[topic].append(websocket)
            self.socket_subscriptions[websocket].add(topic)

    async def unsubscribe(self, websocket: WebSocket, topic: str):
        if topic in self.subscriptions and websocket in self.subscriptions[topic]:
            self.subscriptions[topic].remove(websocket)
        if websocket in self.socket_subscriptions:
            self.socket_subscriptions[websocket].discard(topic)

    async def broadcast_to_topic(self, topic: str, raw_message: str):
        if topic in self.subscriptions:
            try:
                payload = json.loads(raw_message)
            except:
                payload = raw_message
                
            envelope = json.dumps({"topic": topic, "payload": payload})
            
            for connection in list(self.subscriptions[topic]):
                try:
                    await connection.send_text(envelope)
                except:
                    self.disconnect(connection)

websocket_manager = ConnectionManager()