from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
from aiokafka import AIOKafkaConsumer
import json
import uuid
import logging
from app.config import settings

router = APIRouter()
logger = logging.getLogger("uvicorn.info")

@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    logger.info("WebSocket connection established")
    
    consumer = None
    is_connected = True
    
    try:
        # Wait for subscribe
        data = await websocket.receive_json()
        
        if data.get("action") != "subscribe":
            await websocket.send_json({"error": "First message must be subscribe action"})
            return
            
        target_config = data.get("config_name")
        monitor_type = data.get("type", "geofence")
        
        if not target_config:
            await websocket.send_json({"error": "config_name is required"})
            return
        
        # pick topic based on type
        topic_map = {
            "geofence": "robot_geofence_alerts",
            "speed": "robot_speed_alerts",
            "humidity": "robot_humidity_alerts",
            "collision": "robot_collision_alerts"
        }
        
        topic = topic_map.get(monitor_type, "robot_geofence_alerts")
        
        logger.info(f"WebSocket client subscribed to {monitor_type}: {target_config}")
        
        # Subscription confirmation
        await websocket.send_json({
            "status": "subscribed",
            "config_name": target_config,
            "type": monitor_type,
            "topic": topic
        })
        
        bootstrap_servers = settings.KAFKA_BOOTSTRAP_SERVERS
        logger.info(f"Connecting to Kafka at: {bootstrap_servers}")

        # Consumer start
        unique_group_id = f"ws-group-{uuid.uuid4()}"
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=unique_group_id,
            auto_offset_reset="latest",
            enable_auto_commit=False
        )
        
        await consumer.start()
        logger.info(f"Kafka consumer started for {topic}")
        
        # Loop that receives msgs
        async for msg in consumer:
            if not is_connected:
                break
                
            try:
                if msg.value is None:
                    continue
                
                payload = json.loads(msg.value.decode('utf-8'))


                if monitor_type == "humidity":
                    await websocket.send_json(payload)
                    
                elif monitor_type == "collision":
                    await websocket.send_json(payload)

                else:
                    # supports both speed and geofence config formats
                    active_configs_str = payload.get("ACTIVE_CONFIGS") or payload.get("active_configs") or payload.get("zones") or payload.get("ZONES", "")
                    
                    if not active_configs_str:
                        continue
                    
                    active_configs_list = active_configs_str.split("|")
                    
                    if target_config in active_configs_list:
                        try:
                            await websocket.send_json(payload)
                        except Exception:
                            is_connected = False
                            break
                    
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON: {e}")
            except Exception as e:
                logger.error(f"Error processing message: {e}")

    except WebSocketDisconnect:
        is_connected = False
        logger.info("WebSocket disconnected")
    except Exception as e:
        is_connected = False
        logger.error(f"WebSocket error: {e}")
    finally:
        if consumer:
            try:
                await consumer.stop()
                logger.info("Kafka consumer stopped")
            except Exception as e:
                logger.error(f"Error stopping consumer: {e}")