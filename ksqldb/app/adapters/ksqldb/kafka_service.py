import logging
import json
from aiokafka import AIOKafkaProducer
from app.config import settings

logger = logging.getLogger("uvicorn.error")

class KafkaService:
    def __init__(self):
        self.producer = None
        self.broker_url = settings.KAFKA_BOOTSTRAP_SERVERS

    async def start(self):
        logger.info(f"Connecting to Kafka Producer at {self.broker_url}...")
        try:
            self.producer = AIOKafkaProducer(bootstrap_servers=self.broker_url)
            await self.producer.start()
            logger.info("Kafka Producer connected successfully")
        except Exception as e:
            logger.error(f"Failed to connect Kafka Producer: {e}")

    async def stop(self):
        if self.producer:
            await self.producer.stop()

    async def _send_json(self, topic: str, key: str, value: dict, timestamp_ms: int = None):
        """Sends JSON to Kafka"""
        if not self.producer:
            logger.warning("Producer not connected")
            return
        try:
            payload = json.dumps(value).encode('utf-8')
            k = str(key).encode('utf-8')
            
            send_kwargs = {"topic": topic, "key": k, "value": payload}
            if timestamp_ms is not None:
                send_kwargs["timestamp_ms"] = timestamp_ms
            
            await self.producer.send_and_wait(**send_kwargs)
            logger.info(f"Sent to {topic} [{key}]: {value}")
            
        except Exception as e:
            logger.error(f"Kafka send error: {e}")

    # collision control methods
    
    async def send_collision_control_on(self, config_name: str):
        """Activates collision monitoring"""
        payload = {"status": "ON", "config_name": config_name}
        await self._send_json("collision_control", "global", payload)

    async def send_collision_control_off(self):
        """Deactivates collision monitoring"""
        payload = {"status": "OFF", "config_name": "none"}
        await self._send_json("collision_control", "global", payload)

    async def send_robot_selection(self, robot_id: str, status: str):
        """Broadcasts robot selection status to Kafka for the collision detector"""
        payload = {"robot_id": robot_id, "status": status}
        await self._send_json("robot_selection", robot_id, payload)

kafka_service = KafkaService()
def get_kafka_service(): 
    return kafka_service