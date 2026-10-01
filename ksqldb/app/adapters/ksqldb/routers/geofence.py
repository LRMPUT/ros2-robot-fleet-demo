from fastapi import APIRouter, HTTPException, Depends
from app.adapters.ksqldb.schemas import GeofenceRequest
from app.adapters.ksqldb import database
from app.adapters.ksqldb.ksqldb_client import KsqlDBClient, get_ksqldb_client
import logging

router = APIRouter()
logger = logging.getLogger("uvicorn.info")

async def _update_ksqldb_geofence(robot_id: str, hex_list: list, config_names: str, ksql: KsqlDBClient):
    stream_name = f"GEOFENCE_MONITOR_{robot_id.replace('-', '_')}"
    
    # if disabling geofence replace stream with an empty one
    if not hex_list:
        ksql_query = f"""
        CREATE OR REPLACE STREAM {stream_name} WITH (
            KAFKA_TOPIC='robot_geofence_alerts',
            VALUE_FORMAT='JSON'
        ) AS
        SELECT 'geofence' AS `type`, robot_id AS `robot`, timestamp AS `ts`, 
               latitude AS `lat`, longitude AS `lon`, 'OFF' AS `msg`, 'none' AS `zones`
        FROM ROS_GPS_FIX_STREAM
        WHERE 1=0 EMIT CHANGES;
        """
        await ksql.execute_statement(ksql_query)
        return "OFF"
        
    composite_hex = "|".join(hex_list)
    
    # create or replace the monitoring stream
    ksql_query = f"""
    CREATE OR REPLACE STREAM {stream_name} WITH (
        KAFKA_TOPIC='robot_geofence_alerts',
        VALUE_FORMAT='JSON'
    ) AS
    SELECT
        'geofence' AS `type`,
        '{robot_id}' AS `robot`,
        robot_id,
        timestamp AS `ts`,
        latitude AS `lat`,
        longitude AS `lon`,
        'OUTSIDE' AS `msg`,
        '{config_names}' AS `zones`
    FROM ROS_GPS_FIX_STREAM
    WHERE robot_id = '{robot_id}'
      AND INGEOFENCE(latitude, longitude, '{composite_hex}') = false
    EMIT CHANGES;
    """
    
    success = await ksql.execute_statement(ksql_query)
    
    if not success:
        raise HTTPException(500, detail="ksqlDB error, check logs with docker compose logs api")
        
    return f"ON (updated: {config_names})"

@router.post("/geofence", tags=["Control"])  
async def add_geofence_rule(data: GeofenceRequest, ksql: KsqlDBClient = Depends(get_ksqldb_client)):
    robot_exists = database.get_robot(data.robot_id)
    if not robot_exists:
         raise HTTPException(404, detail=f"Robot '{data.robot_id}' does not exist.")
    zone = database.get_zone(data.zone_id)
    if not zone:
        raise HTTPException(404, detail=f"Zone '{data.zone_id}' not found.")

    database.add_geofence_assignment(data.robot_id, data.zone_id, data.config_name)
    all_assignments = database.get_all_assignments_for_robot(data.robot_id)
    
    hex_list = [item['geo'] for item in all_assignments]
    composite_configs = "|".join(list(set([item['config_name'] for item in all_assignments])))
    
    await _update_ksqldb_geofence(data.robot_id, hex_list, composite_configs, ksql)
    return {"status": "assigned", "robot_id": data.robot_id, "active_configs": composite_configs}

@router.delete("/geofence", tags=["Control"])
async def remove_geofence_rule(data: GeofenceRequest, ksql: KsqlDBClient = Depends(get_ksqldb_client)):
    database.remove_geofence_assignment(data.robot_id, data.config_name)
    remaining = database.get_all_assignments_for_robot(data.robot_id)
    
    hex_list = [item['geo'] for item in remaining]
    composite_configs = "|".join(list(set([item['config_name'] for item in remaining])))
    
    status = await _update_ksqldb_geofence(data.robot_id, hex_list, composite_configs, ksql)
    return {"status": "removed", "robot_id": data.robot_id, "ksqldb_status": status}

@router.delete("/geofence/reset/{robot_id}", tags=["Control"])
async def reset_geofence(robot_id: str, ksql: KsqlDBClient = Depends(get_ksqldb_client)):
    database.remove_all_geofence_assignments_for_robot(robot_id)
    await _update_ksqldb_geofence(robot_id, [], "", ksql)
    return {"status": "reset_complete", "robot_id": robot_id}

# list endpoints
@router.get("/geofence", tags=["Control"])
def list_geofence_rules():
    return database.get_all_assignments()