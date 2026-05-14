import sqlite3
import logging
from typing import List, Optional
import json
from app.config import settings

# Database name
DB_NAME = settings.KSQLDB_DB_NAME
logger = logging.getLogger("uvicorn.info")

def get_connection():
    """Creates connection to SQLite3"""
    conn = sqlite3.connect(DB_NAME)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    try:
        with get_connection() as conn:
            # Robots table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS robots (
                    id TEXT PRIMARY KEY,
                    status TEXT
                )
            ''')
            # Zone table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS zones (
                    id TEXT PRIMARY KEY,
                    geo TEXT
                )
            ''')
            # Configurations, config table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS configurations (
                    name TEXT PRIMARY KEY, 
                    config_json TEXT,
                    control_topic TEXT,
                    output_topic TEXT  
                );
            ''')
            # Assignments who, where, in what config
            conn.execute('''
                CREATE TABLE IF NOT EXISTS geofence_assignments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    robot_id TEXT,
                    zone_id TEXT,
                    config_name TEXT
                );
            ''')
            # Speed assignments table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS speed_assignments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    robot_id TEXT NOT NULL,
                    config_name TEXT NOT NULL,
                    UNIQUE(robot_id, config_name)
                );
            ''')

            # SENSORS
            conn.execute('''
                CREATE TABLE IF NOT EXISTS sensors (
                    sensor_id TEXT PRIMARY KEY
                )
            ''')
            
            # HUMIDITY RULES
            conn.execute('''
                CREATE TABLE IF NOT EXISTS humidity_rules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sensor_id TEXT NOT NULL,
                    min_humidity REAL NOT NULL,
                    alert_radius_m REAL NOT NULL,
                    config_name TEXT NOT NULL,
                    UNIQUE(sensor_id, config_name),
                    FOREIGN KEY(sensor_id) REFERENCES sensors(sensor_id)
                )
            ''')

            logger.info(f"Database ({DB_NAME}) initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise

# ROBOTS
class RobotEntry:
    def __init__(self, id, status):
        self.id = id
        self.status = status

def upsert_robot(robot: RobotEntry):
    with get_connection() as conn:
        conn.execute('INSERT OR REPLACE INTO robots (id, status) VALUES (?, ?)', (robot.id, robot.status))

def get_all_robots() -> List[dict]:
    with get_connection() as conn:
        rows = conn.execute("SELECT * FROM robots").fetchall()
        return [dict(row) for row in rows]

def get_robot(robot_id: str) -> Optional[dict]:
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM robots WHERE id = ?", (robot_id,)).fetchone()
        return dict(row) if row else None
    
def delete_robot(robot_id: str):
    with get_connection() as conn:
        conn.execute("DELETE FROM geofence_assignments WHERE robot_id = ?", (robot_id,))
        conn.execute("DELETE FROM robots WHERE id = ?", (robot_id,))

# ZONES
class ZoneEntry:
    def __init__(self, id, geo):
        self.id = id
        self.geo = geo

def upsert_zone(zone: ZoneEntry):
    with get_connection() as conn:
        conn.execute('INSERT OR REPLACE INTO zones (id, geo) VALUES (?, ?)', (zone.id, zone.geo))

def get_all_zones() -> List[dict]:
    with get_connection() as conn:
        rows = conn.execute("SELECT * FROM zones").fetchall()
        return [dict(row) for row in rows]

def get_zone(zone_id: str) -> Optional[dict]:
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM zones WHERE id = ?", (zone_id,)).fetchone()
        return dict(row) if row else None

def delete_zone(zone_id: str):
    with get_connection() as conn:
        conn.execute("DELETE FROM geofence_assignments WHERE zone_id = ?", (zone_id,))
        conn.execute("DELETE FROM zones WHERE id = ?", (zone_id,))

# ASSIGNMENTS

def add_geofence_assignment(robot_id: str, zone_id: str, config_name: str):
    with get_connection() as conn:
        exists = conn.execute(
            "SELECT 1 FROM geofence_assignments WHERE robot_id=? AND zone_id=? AND config_name=?",
            (robot_id, zone_id, config_name)
        ).fetchone()
        
        if not exists:
            conn.execute(
                "INSERT INTO geofence_assignments (robot_id, zone_id, config_name) VALUES (?, ?, ?)",
                (robot_id, zone_id, config_name)
            )

def remove_geofence_assignment(robot_id: str, config_name: str):
    with get_connection() as conn:
        conn.execute("DELETE FROM geofence_assignments WHERE robot_id=? AND config_name=?", (robot_id, config_name))

def get_all_assignments_for_robot(robot_id: str) -> List[dict]:
    """Get all geofence assignments for a robot with zone geometry"""
    assignments = []
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT zone_id, config_name FROM geofence_assignments WHERE robot_id = ?", 
            (robot_id,)
        ).fetchall()
        
        for row in rows:
            z_id = row['zone_id']
            c_name = row['config_name']
            zone_row = conn.execute("SELECT geo FROM zones WHERE id = ?", (z_id,)).fetchone()
            if zone_row:
                assignments.append({
                    "id": z_id, 
                    "geo": zone_row['geo'], 
                    "config_name": c_name
                })
    return assignments

def get_all_assignments() -> List[dict]:
    """Get all geofence assignments"""
    with get_connection() as conn:
        rows = conn.execute("SELECT * FROM geofence_assignments").fetchall()
        return [dict(row) for row in rows]

def remove_all_geofence_assignments_for_robot(robot_id: str):
    """Remove all geofence assignments for a robot"""
    with get_connection() as conn:
        conn.execute(
            "DELETE FROM geofence_assignments WHERE robot_id=?",
            (robot_id,)
        )
        logger.info(f"HARD RESET: Removed all geofence assignments for {robot_id}")

# SPEED ASSIGNMENTS

def add_speed_assignment(robot_id: str, config_name: str):
    """Add a speed assignment"""
    with get_connection() as conn:
        exists = conn.execute(
            "SELECT 1 FROM speed_assignments WHERE robot_id=? AND config_name=?",
            (robot_id, config_name)
        ).fetchone()
        
        if not exists:
            conn.execute(
                "INSERT INTO speed_assignments (robot_id, config_name) VALUES (?, ?)",
                (robot_id, config_name)
            )
            logger.info(f"Added speed assignment: {robot_id} -> {config_name}")

def remove_speed_assignment(robot_id: str, config_name: str):
    """Remove a speed assignment"""
    with get_connection() as conn:
        conn.execute(
            "DELETE FROM speed_assignments WHERE robot_id=? AND config_name=?",
            (robot_id, config_name)
        )
        logger.info(f"Removed speed assignment: {robot_id} -> {config_name}")

def get_all_speed_assignments() -> List[dict]:
    """Get all speed assignments"""
    with get_connection() as conn:
        rows = conn.execute("SELECT * FROM speed_assignments").fetchall()
        return [dict(row) for row in rows]

def get_speed_assignments_for_robot(robot_id: str) -> List[dict]:
    """Get speed assignments for a robot"""
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM speed_assignments WHERE robot_id=?",
            (robot_id,)
        ).fetchall()
        return [dict(row) for row in rows]

def remove_all_speed_assignments_for_robot(robot_id: str):
    """Remove all speed assignments for a robot"""
    with get_connection() as conn:
        conn.execute(
            "DELETE FROM speed_assignments WHERE robot_id=?",
            (robot_id,)
        )
        logger.info(f"HARD RESET: Removed all speed assignments for {robot_id}")

# SENSORS
class SensorEntry:
    def __init__(self, sensor_id, description=""):
        self.sensor_id = sensor_id

def upsert_sensor(sensor: SensorEntry):
    with get_connection() as conn:
        conn.execute(
            'INSERT OR REPLACE INTO sensors (sensor_id) VALUES (?)',
            (sensor.sensor_id,)
        )

def get_all_sensors() -> List[dict]:
    with get_connection() as conn:
        rows = conn.execute("SELECT * FROM sensors").fetchall()
        return [dict(row) for row in rows]

def get_sensor(sensor_id: str) -> Optional[dict]:
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM sensors WHERE sensor_id = ?", (sensor_id,)).fetchone()
        return dict(row) if row else None

def delete_sensor(sensor_id: str):
    with get_connection() as conn:
        conn.execute("DELETE FROM humidity_rules WHERE sensor_id = ?", (sensor_id,))
        conn.execute("DELETE FROM sensors WHERE sensor_id = ?", (sensor_id,))

# HUMIDITY RULES
def add_humidity_rule(sensor_id: str, min_humidity: float, alert_radius_m: float, config_name: str):
    with get_connection() as conn:
        exists = conn.execute(
            "SELECT 1 FROM humidity_rules WHERE sensor_id=? AND config_name=?",
            (sensor_id, config_name)
        ).fetchone()
        
        if not exists:
            conn.execute(
                "INSERT INTO humidity_rules (sensor_id, min_humidity, alert_radius_m, config_name) VALUES (?, ?, ?, ?)",
                (sensor_id, min_humidity, alert_radius_m, config_name)
            )
            logger.info(f"Added humidity rule: sensor {sensor_id} -> {config_name}")

def remove_humidity_rule(sensor_id: str, config_name: str):
    with get_connection() as conn:
        conn.execute(
            "DELETE FROM humidity_rules WHERE sensor_id=? AND config_name=?",
            (sensor_id, config_name)
        )
        logger.info(f"Removed humidity rule: sensor {sensor_id} -> {config_name}")

def get_all_humidity_rules() -> List[dict]:
    with get_connection() as conn:
        rows = conn.execute("SELECT * FROM humidity_rules").fetchall()
        return [dict(row) for row in rows]

def get_humidity_rules_for_sensor(sensor_id: str) -> List[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM humidity_rules WHERE sensor_id=?",
            (sensor_id,)
        ).fetchall()
        return [dict(row) for row in rows]