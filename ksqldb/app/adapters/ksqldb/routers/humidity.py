from fastapi import APIRouter, HTTPException, Depends
from app.adapters.ksqldb.schemas import SensorCreate, HumidityRuleRequest
from app.adapters.ksqldb import database
from app.adapters.ksqldb.ksqldb_client import KsqlDBClient, get_ksqldb_client
import logging

router = APIRouter()
logger = logging.getLogger("uvicorn.info")

async def _update_ksqldb_humidity(sensor_id: str, min_humidity: float, radius_m: float, ksql: KsqlDBClient):
    """Create or update the ksqlDB humidity monitoring stream for a sensor"""
    
    # make sure the base sensor stream exists
    base_stream_query = """
    CREATE STREAM IF NOT EXISTS SENSOR_PROXIMITY_STREAM (
        sensor_id VARCHAR KEY,
        timestamp BIGINT,
        position_x DOUBLE,
        position_y DOUBLE,
        humidity DOUBLE
    ) WITH (
        KAFKA_TOPIC='sensor_proximity',
        VALUE_FORMAT='JSON',
        TIMESTAMP='timestamp',
        PARTITIONS=2
    );
    """
    await ksql.execute_statement(base_stream_query)

    stream_name = f"HUMIDITY_ALERTS_{sensor_id.replace('-', '_')}"
    table_state = f"SENSOR_STATE_{sensor_id.replace('-', '_')}"
    
    # disable by replacing stream with an empty one
    if min_humidity == 0.0 and radius_m == 0.0:
        await ksql.execute_statement(f"""
        CREATE OR REPLACE STREAM {stream_name} WITH (
            KAFKA_TOPIC='robot_humidity_alerts', VALUE_FORMAT='JSON'
        ) AS SELECT 'humidity' AS `type` FROM ROS_GPS_FIX_STREAM WHERE 1=0 EMIT CHANGES;
        """)
        return "OFF"
        
    # create or replace the sensor state table
    ksql_table = f"""
    CREATE OR REPLACE TABLE {table_state} AS
    SELECT 
        sensor_id,
        LATEST_BY_OFFSET(position_x) AS lon,
        LATEST_BY_OFFSET(position_y) AS lat,
        LATEST_BY_OFFSET(humidity) AS current_humidity,
        LATEST_BY_OFFSET(timestamp) AS last_ts
    FROM SENSOR_PROXIMITY_STREAM
    WHERE sensor_id = '{sensor_id}'
    GROUP BY sensor_id;
    """
    await ksql.execute_statement(ksql_table)
    
    # create or replace the alert stream with join
    ksql_stream = f"""
    CREATE OR REPLACE STREAM {stream_name} WITH (
        KAFKA_TOPIC='robot_humidity_alerts',
        VALUE_FORMAT='JSON'
    ) AS
    SELECT
        'humidity' AS `type`,
        r.robot_id AS `robot`,
        r.timestamp AS `ts`,
        r.latitude AS `lat`,
        r.longitude AS `lon`,
        '{sensor_id}' AS `sensor`,
        s.sensor_id,  -- required by ksqlDB join engine
        s.current_humidity AS `humidity`,
        {min_humidity} AS `threshold`,
        GEO_DISTANCE(r.latitude, r.longitude, s.lat, s.lon) * 1000 AS `distance_m`
    FROM ROS_GPS_FIX_STREAM r
    JOIN {table_state} s 
      ON s.sensor_id = CASE WHEN r.robot_id IS NOT NULL THEN '{sensor_id}' ELSE 'unknown' END
    WHERE (GEO_DISTANCE(r.latitude, r.longitude, s.lat, s.lon) * 1000) < {radius_m}
      AND s.current_humidity > {min_humidity}
      AND s.last_ts > (r.timestamp - 1800000)
    EMIT CHANGES;
    """

    success = await ksql.execute_statement(ksql_stream)
    if not success:
        logger.error(f"Failed to create humidity rule for sensor {sensor_id}")
    return "ON"


# SENSORS
@router.post("/sensors", tags=["Humidity Management"])
async def add_sensor(sensor: SensorCreate):
    database.upsert_sensor(database.SensorEntry(sensor.sensor_id))
    return {"status": "registered", "sensor_id": sensor.sensor_id}

@router.get("/sensors", tags=["Humidity Management"])
def list_sensors():
    return database.get_all_sensors()

@router.get("/sensors/{sensor_id}", tags=["Humidity Management"])
def get_sensor(sensor_id: str):
    sensor = database.get_sensor(sensor_id)
    if not sensor:
        raise HTTPException(404, detail=f"Sensor {sensor_id} not found")
    return sensor

@router.delete("/sensors/{sensor_id}", tags=["Humidity Management"])
async def delete_sensor(sensor_id: str, ksql: KsqlDBClient = Depends(get_ksqldb_client)):
    # disable the ksqlDB stream when deleting sensor
    await _update_ksqldb_humidity(sensor_id, 0.0, 0.0, ksql)
    database.delete_sensor(sensor_id)
    return {"status": "deleted", "sensor_id": sensor_id}


# RULES CONTROL
@router.post("/humidity", tags=["Humidity Control"])
async def add_humidity_rule(data: HumidityRuleRequest, ksql: KsqlDBClient = Depends(get_ksqldb_client)):
    sensor = database.get_sensor(data.sensor_id)
    if not sensor:
        raise HTTPException(404, detail=f"Sensor {data.sensor_id} not found. Register it first via POST /sensors.")
    
    database.add_humidity_rule(data.sensor_id, data.min_humidity, data.alert_radius_m, data.config_name)
    
    # update ksqlDB streams
    await _update_ksqldb_humidity(data.sensor_id, data.min_humidity, data.alert_radius_m, ksql)
    
    return {
        "status": "rule_activated",
        "sensor_id": data.sensor_id,
        "config_name": data.config_name,
        "threshold": data.min_humidity,
        "radius": data.alert_radius_m
    }

@router.delete("/humidity", tags=["Humidity Control"])
async def remove_humidity_rule(data: HumidityRuleRequest, ksql: KsqlDBClient = Depends(get_ksqldb_client)):
    database.remove_humidity_rule(data.sensor_id, data.config_name)
    
    # disable the stream
    await _update_ksqldb_humidity(data.sensor_id, 0.0, 0.0, ksql)
    
    return {"status": "rule_deactivated", "sensor_id": data.sensor_id, "config_name": data.config_name}

@router.get("/humidity", tags=["Humidity Control"])
def list_humidity_rules():
    return database.get_all_humidity_rules()