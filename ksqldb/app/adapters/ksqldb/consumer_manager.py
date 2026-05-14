import asyncio
import logging
from aiokafka import AIOKafkaConsumer
from app.config import settings
from .websocket_manager import websocket_manager

logger = logging.getLogger("uvicorn.info")

class ConsumerManager:
    """Manages shared kafka consumers for websocket topics"""
    def __init__(self):
        self.active_tasks = {}

    async def start_consumer(self, topic_name: str):
        """Start or reuse a consumer for the given topic"""
        if topic_name in self.active_tasks:
            self.active_tasks[topic_name]["count"] += 1
            return

        logger.info(f"Starting consumer for topic: {topic_name}")
        
        consumer = AIOKafkaConsumer(
            topic_name,
            bootstrap_servers=settings.KAFKA_BROKER,
            group_id=f"ws-group-{topic_name}",
            auto_offset_reset='latest'
        )
        await consumer.start()
        
        task = asyncio.create_task(self._consume_loop(topic_name, consumer))
        self.active_tasks[topic_name] = {"task": task, "consumer": consumer, "count": 1}

    async def _consume_loop(self, topic_name: str, consumer: AIOKafkaConsumer):
        """Read messages from kafka and broadcast to websockets"""
        try:
            async for msg in consumer:
                val = msg.value.decode('utf-8')
                await websocket_manager.broadcast_to_topic(topic_name, val)
        except Exception as e:
            logger.error(f"Consumer error {topic_name}: {e}")
        finally:
            await consumer.stop()

    async def stop_consumer(self, topic_name: str):
        """Decrement ref count and stop consumer if no longer needed"""
        if topic_name in self.active_tasks:
            self.active_tasks[topic_name]["count"] -= 1
            if self.active_tasks[topic_name]["count"] <= 0:
                entry = self.active_tasks.pop(topic_name)
                entry["task"].cancel()
                await entry["consumer"].stop()

    async def stop_all(self):
        """Stop all active consumers"""
        for topic in list(self.active_tasks.keys()):
            await self.stop_consumer(topic)

consumer_manager = ConsumerManager()