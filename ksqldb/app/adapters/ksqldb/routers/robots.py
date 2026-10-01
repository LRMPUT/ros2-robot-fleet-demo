from fastapi import APIRouter, HTTPException, Depends
from app.adapters.ksqldb import database
from app.adapters.ksqldb.schemas import RobotCreate
from app.adapters.ksqldb.kafka_service import KafkaService, get_kafka_service

router = APIRouter()

@router.post("/robots", tags=["Management"])
async def select_robot(robot: RobotCreate, kafka: KafkaService = Depends(get_kafka_service)):
    database.upsert_robot(database.RobotEntry(robot.id, "SELECTED"))
    await kafka.send_robot_selection(robot.id, "SELECTED")
    return {"status": "selected", "id": robot.id}

@router.get("/robots", tags=["Management"])
def list_robots():
    return database.get_all_robots()

@router.delete("/robots/{robot_id}", tags=["Management"])
async def deselect_robot(robot_id: str, kafka: KafkaService = Depends(get_kafka_service)):
    database.delete_robot(robot_id)
    await kafka.send_robot_selection(robot_id, "DESELECTED")
    return {"status": "deselected"}