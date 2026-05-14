from pydantic import BaseModel, Field
from typing import Optional

class GeofenceRequest(BaseModel):
    robot_id: Optional[str] = Field(None, description="The specific robot to track")
    zone_id: Optional[str] = Field(None, description="The zone ID")
    config_name: str = Field(..., min_length=1, description="Name of the configuration profile")

class ZoneCreate(BaseModel):
    id: str = Field(..., min_length=1, description="Unique Zone Identifier")
    geo: str = Field(..., min_length=3, description="PostGIS EWKB HEX")

class RobotCreate(BaseModel):
    id: str = Field(..., min_length=1, description="Unique Robot ID")

class SensorCreate(BaseModel):
    sensor_id: str = Field(..., min_length=1, description="ID of the sensor")

class HumidityRuleRequest(BaseModel):
    sensor_id: str = Field(..., min_length=1, description="ID of the sensor")
    min_humidity: float = Field(..., ge=0, le=100, description="Minimum humidity threshold (0-100%)")
    alert_radius_m: float = Field(..., gt=0, description="Alert radius in meters")
    config_name: str = Field(..., description="Configuration name for grouping rules")