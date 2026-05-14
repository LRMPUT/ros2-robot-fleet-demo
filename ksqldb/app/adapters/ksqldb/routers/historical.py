from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List
from pydantic import BaseModel
import logging
from app.adapters.ksqldb import database
from app.common.rosbag.reader import ROSBagGeofenceReader
from app.common.models import GPSPoint

router = APIRouter()
logger = logging.getLogger("uvicorn.info")

# ROSbag path from docker-compose volume
ROSBAG_PATH = "/bags/rorbots_follower_leader_parcelle_1MONT_ros2"

class HistoricalGeofenceResponse(BaseModel):
    robot_id: str
    zone_id: str
    zone_name: str
    total_points_analyzed: int
    points_outside_count: int
    percentage_outside: float
    duration_seconds: float
    start_time: float
    end_time: float
    first_violation_time: Optional[float]
    last_violation_time: Optional[float]
    gps_coordinates_outside: List[GPSPoint]

class GeofenceSummary(BaseModel):
    robot_id: str
    zone_id: str
    zone_name: str
    total_points_analyzed: int
    points_outside_count: int
    percentage_outside: float
    duration_seconds: float
    start_time: float
    end_time: float
    first_violation_time: Optional[float]
    last_violation_time: Optional[float]


@router.get(
    "/historical/geofence", 
    tags=["Historical Queries"],
    response_model=HistoricalGeofenceResponse,
    summary="GPS coordinates outside zone"
)
def get_historical_geofence_violations(
    robot_id: str = Query(..., description="Robot ID (e.g., 'leader', 'follower')"),
    zone_id: str = Query(..., description="Zone ID from database"),
    limit: Optional[int] = Query(None, description="Max number of points to return (None = all)")
):
    """Return all the GPS coordinates where the robot is out from the specified plot"""
    logger.info(f"Historical query: robot={robot_id}, zone={zone_id}, limit={limit}")
    
    # Get zone from database
    zone = database.get_zone(zone_id)
    if not zone:
        raise HTTPException(404, detail=f"Zone '{zone_id}' not found in database")
    
    polygon_hex = zone['geo']
    zone_name = zone.get('name', zone_id)
    
    # Read ROSbag
    try:
        reader = ROSBagGeofenceReader(ROSBAG_PATH)
        result = reader.get_gps_outside_zone(robot_id, polygon_hex, limit)
    except FileNotFoundError as e:
        raise HTTPException(404, detail=str(e))
    except ValueError as e:
        raise HTTPException(400, detail=str(e))
    except Exception as e:
        logger.error(f"Error reading ROSbag: {e}")
        raise HTTPException(500, detail=f"Failed to process ROSbag: {str(e)}")
    
    # Format result
    total = result["total_points"]
    outside = result["count_outside"]
    percentage = (outside / total * 100) if total > 0 else 0.0
    
    # Convert points to Pydantic models (sorted by timestamp)
    gps_points = [
        GPSPoint(
            timestamp=point["timestamp"],
            latitude=point["latitude"],
            longitude=point["longitude"]
        )
        for point in sorted(result["points_outside"], key=lambda x: x["timestamp"])
    ]
    
    return HistoricalGeofenceResponse(
        robot_id=robot_id,
        zone_id=zone_id,
        zone_name=zone_name,
        total_points_analyzed=total,
        points_outside_count=outside,
        percentage_outside=round(percentage, 2),
        duration_seconds=result["duration_seconds"],
        start_time=result["start_time"],
        end_time=result["end_time"],
        first_violation_time=result.get("first_violation_time"),
        last_violation_time=result.get("last_violation_time"),
        gps_coordinates_outside=gps_points
    )


@router.get(
    "/historical/geofence/summary", 
    tags=["Historical Queries"],
    response_model=GeofenceSummary,
    summary="Quick summary - statistics only"
)
def get_historical_geofence_summary(
    robot_id: str = Query(..., description="Robot ID"),
    zone_id: str = Query(..., description="Zone ID")
):
    """Quick summary"""

    zone = database.get_zone(zone_id)
    if not zone:
        raise HTTPException(404, detail=f"Zone '{zone_id}' not found")
    
    try:
        reader = ROSBagGeofenceReader(ROSBAG_PATH)
        result = reader.get_gps_outside_zone(robot_id, zone['geo'], limit=None)
    except Exception as e:
        logger.error(f"Error: {e}")
        raise HTTPException(500, detail=str(e))
    
    # Only for statistics
    total = result["total_points"]
    outside = result["count_outside"]
    percentage = (outside / total * 100) if total > 0 else 0.0
    
    return GeofenceSummary(
        robot_id=robot_id,
        zone_id=zone_id,
        zone_name=zone.get('name', zone_id),
        total_points_analyzed=total,
        points_outside_count=outside,
        percentage_outside=round(percentage, 2),
        duration_seconds=result["duration_seconds"],
        start_time=result["start_time"],
        end_time=result["end_time"],
        first_violation_time=result.get("first_violation_time"),
        last_violation_time=result.get("last_violation_time")   
    )


@router.get(
    "/historical/geofence/csv",
    tags=["Historical Queries"],
    summary="Export to CSV format"
)
def export_geofence_violations_csv(
    robot_id: str = Query(..., description="Robot ID"),
    zone_id: str = Query(..., description="Zone ID"),
    limit: Optional[int] = Query(None, description="Max rows")
):
    """Export to CSV"""
    from fastapi.responses import Response
    
    zone = database.get_zone(zone_id)
    if not zone:
        raise HTTPException(404, detail=f"Zone '{zone_id}' not found")
    
    try:
        reader = ROSBagGeofenceReader(ROSBAG_PATH)
        result = reader.get_gps_outside_zone(robot_id, zone['geo'], limit)
    except Exception as e:
        raise HTTPException(500, detail=str(e))
    
    # Generate CSV
    csv_lines = ["timestamp,latitude,longitude"]
    for point in sorted(result["points_outside"], key=lambda x: x["timestamp"]):
        csv_lines.append(f"{point['timestamp']},{point['latitude']},{point['longitude']}")
    
    csv_content = "\n".join(csv_lines)
    
    return Response(
        content=csv_content,
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename=geofence_{robot_id}_{zone_id}.csv"
        }
    )