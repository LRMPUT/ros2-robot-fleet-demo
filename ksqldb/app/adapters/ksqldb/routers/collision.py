from fastapi import APIRouter, HTTPException, Depends
from app.adapters.ksqldb import database
from app.adapters.ksqldb.kafka_service import KafkaService, get_kafka_service
from pydantic import BaseModel, Field
from typing import List, Dict
import logging

router = APIRouter()
logger = logging.getLogger("uvicorn.info")

# Request model for collision activation
class CollisionControlRequest(BaseModel):
    config_name: str = Field(..., min_length=1, description="Configuration name for this collision monitoring session")


@router.post("/collision/activate", tags=["Collision Detection"])
async def activate_collision_monitoring(
    data: CollisionControlRequest,
    kafka: KafkaService = Depends(get_kafka_service)
):
    """Activate collision detection for all registered robots"""
    # Check if we have at least 2 registered robots
    registered_robots = database.get_all_robots()
    active_robots = [r for r in registered_robots if r.get('status') == 'SELECTED']
    
    if len(active_robots) < 2:
        raise HTTPException(
            400, 
            detail=f"Need at least 2 registered robots for collision detection. Currently have {len(active_robots)}."
        )
    
    # Send activation message to collision_control topic
    await kafka.send_collision_control_on(data.config_name)
    
    logger.warning(f"COLLISION MONITORING ACTIVATED | Config: {data.config_name}")
    
    return {
        "status": "activated",
        "config_name": data.config_name,
        "monitoring_robots": [r['id'] for r in active_robots],
        "robot_count": len(active_robots),
        "control_topic": "collision_control"
    }


@router.post("/collision/deactivate", tags=["Collision Detection"])
async def deactivate_collision_monitoring(
    kafka: KafkaService = Depends(get_kafka_service)
):
    """Deactivate collision detection"""
    await kafka.send_collision_control_off()
    
    logger.warning(f"COLLISION MONITORING DEACTIVATED")
    
    return {
        "status": "deactivated",
        "message": "Collision monitoring has been turned off"
    }


@router.get("/collision/status", tags=["Collision Detection"])
def get_collision_status():
    """Get current collision detection status"""
    registered_robots = database.get_all_robots()
    
    # Filter only SELECTED robots
    active_robots = [r for r in registered_robots if r.get('status') == 'SELECTED']
    
    monitoring_possible = len(active_robots) >= 2
    
    return {
        "system": "collision_detection",
        "monitoring_possible": monitoring_possible,
        "registered_robots_count": len(active_robots),
        "registered_robots": [r['id'] for r in active_robots],
        "minimum_required": 2,
        "configuration": {
            "threshold_m": "Set in collision_detector.py (COLLISION_DISTANCE_THRESHOLD_M)",
            "message_expiration_s": "Set in collision_detector.py (MESSAGE_EXPIRATION_TIME_S)",
            "control_topic": "collision_control"
        },
        "usage": {
            "activate": "POST /ksqldb/collision/activate with config_name",
            "deactivate": "POST /ksqldb/collision/deactivate"
        },
        "note": "Collision monitoring must be explicitly activated via API. It will only monitor registered robot pairs when turned ON."
    }

@router.get("/collision/info", tags=["Collision Detection"])
def get_collision_info():
    """Information about the collision detection system"""
    return {
        "description": "Controlled all-to-all collision detection for registered robots",
        "algorithm": "Haversine distance calculation between GPS positions",
        "how_it_works": [
            "1. System waits in standby mode (monitoring disabled)",
            "2. API sends activation message to collision_control topic",
            "3. Detector reads robot_registration topic to track active robots",
            "4. Maintains latest GPS position for each registered robot",
            "5. Periodically compares all robot pairs",
            "6. Sends alert to robot_collision_alerts if distance < threshold",
            "7. API can deactivate monitoring at any time"
        ],
        "control_flow": {
            "activate": "POST /ksqldb/collision/activate → collision_control topic (status=ON)",
            "deactivate": "POST /ksqldb/collision/deactivate → collision_control topic (status=OFF)"
        },
        "websocket_endpoint": "/ksqldb/ws",
        "websocket_subscribe": {
            "action": "subscribe",
            "type": "collision",
            "config_name": "your_config_name"
        },
        "configuration_location": "scripts/collision_detector.py (edit threshold and restart service)"
    }