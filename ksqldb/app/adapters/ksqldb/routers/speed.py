from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import List
from app.adapters.ksqldb import database
from app.adapters.ksqldb.ksqldb_client import KsqlDBClient, get_ksqldb_client
import logging

router = APIRouter()
logger = logging.getLogger("uvicorn.info")

class SpeedRequest(BaseModel):
    robot_id: str
    config_name: str

class SpeedAssignment(BaseModel):
    id: int
    robot_id: str
    config_name: str
    status: str = "ACTIVE"

class SpeedListResponse(BaseModel):
    total_count: int
    assignments: List[SpeedAssignment]

async def _update_ksqldb_speed(robot_id: str, config_names: str, ksql: KsqlDBClient):
    table_name = f"SPEED_ALERTS_{robot_id.replace('-', '_')}"
    
    # disable by replacing with empty table
    if not config_names:
        ksql_query = f"""
        CREATE OR REPLACE TABLE {table_name} WITH (
            KAFKA_TOPIC='robot_speed_alerts',
            VALUE_FORMAT='JSON'
        ) AS
        SELECT robot_id, LATEST_BY_OFFSET('OFF') AS active_configs
        FROM ROS_FILTERED_ODOM_STREAM
        WHERE 1=0
        GROUP BY robot_id
        EMIT CHANGES;
        """
        await ksql.execute_statement(ksql_query)
        return "OFF"
        
    # create or replace the speed monitoring table
    ksql_query = f"""
    CREATE OR REPLACE TABLE {table_name} WITH (
        KAFKA_TOPIC='robot_speed_alerts',
        VALUE_FORMAT='JSON'
    ) AS
    SELECT
        robot_id,
        CAST(robot_id AS VARCHAR) AS robot_name,
        TIMESTAMPTOSTRING(WINDOWSTART, 'HH:mm:ss') AS window_start,
        TIMESTAMPTOSTRING(WINDOWEND, 'HH:mm:ss') AS window_end,
        
        LATEST_BY_OFFSET(position_x) AS current_x,
        LATEST_BY_OFFSET(position_y) AS current_y,
        '{config_names}' AS active_configs,
        
        SQRT(
            POWER(LATEST_BY_OFFSET(position_x) - EARLIEST_BY_OFFSET(position_x), 2) +
            POWER(LATEST_BY_OFFSET(position_y) - EARLIEST_BY_OFFSET(position_y), 2) +
            POWER(LATEST_BY_OFFSET(position_z) - EARLIEST_BY_OFFSET(position_z), 2)
        ) / ((MAX(timestamp) - MIN(timestamp)) / 1000.0) AS avg_speed_mps,

        (MAX(timestamp) - MIN(timestamp)) / 1000.0 AS time_window_s,
        COUNT(*) AS sample_count
    FROM ROS_FILTERED_ODOM_STREAM
    WINDOW HOPPING (SIZE 2 SECONDS, ADVANCE BY 1 SECOND)
    WHERE robot_id = '{robot_id}' AND timestamp IS NOT NULL
    GROUP BY robot_id
    HAVING COUNT(*) >= 2 AND (MAX(timestamp) - MIN(timestamp)) > 0
    EMIT CHANGES;
    """
    await ksql.execute_statement(ksql_query)
    return "ON"

@router.post("/speed", tags=["Control"])
async def enable_speed_monitoring(data: SpeedRequest, ksql: KsqlDBClient = Depends(get_ksqldb_client)):
    robot_exists = database.get_robot(data.robot_id)
    if not robot_exists:
        raise HTTPException(404, detail=f"Robot '{data.robot_id}' does not exist.")
    
    database.add_speed_assignment(data.robot_id, data.config_name)
    all_assignments = database.get_speed_assignments_for_robot(data.robot_id)
    composite_configs = "|".join([item['config_name'] for item in all_assignments])
    
    await _update_ksqldb_speed(data.robot_id, composite_configs, ksql)
    return {"status": "enabled", "robot_id": data.robot_id, "active_configs_sent": composite_configs}

@router.delete("/speed", tags=["Control"])
async def disable_speed_monitoring(data: SpeedRequest, ksql: KsqlDBClient = Depends(get_ksqldb_client)):
    database.remove_speed_assignment(data.robot_id, data.config_name)
    remaining = database.get_speed_assignments_for_robot(data.robot_id)
    
    composite_configs = "|".join([r['config_name'] for r in remaining]) if remaining else ""
    await _update_ksqldb_speed(data.robot_id, composite_configs, ksql)
    return {"status": "updated", "ksqldb_status": "OFF" if not remaining else "ON"}

@router.delete("/speed/reset/{robot_id}", tags=["Control"])
async def reset_robot_monitoring(robot_id: str, ksql: KsqlDBClient = Depends(get_ksqldb_client)):
    database.remove_all_speed_assignments_for_robot(robot_id)
    await _update_ksqldb_speed(robot_id, "", ksql)
    return {"status": "reset_complete", "robot_id": robot_id}

# list endpoints
@router.get("/speed", tags=["Control"], response_model=SpeedListResponse)
def list_speed_monitoring():
    assignments = database.get_all_speed_assignments()
    assignment_list = [
        SpeedAssignment(id=a['id'], robot_id=a['robot_id'], config_name=a['config_name'], status="ACTIVE")
        for a in assignments
    ]
    return SpeedListResponse(total_count=len(assignment_list), assignments=assignment_list)

@router.get("/speed/{robot_id}", tags=["Control"])
def get_speed_monitoring_for_robot(robot_id: str):
    assignments = database.get_speed_assignments_for_robot(robot_id)
    if not assignments:
        return {"robot_id": robot_id, "is_monitored": False, "config_name": None}
    return {
        "robot_id": robot_id,
        "is_monitored": True,
        "config_name": assignments[0]['config_name'], 
        "active_configs_count": len(assignments),
        "assignments": assignments
    }