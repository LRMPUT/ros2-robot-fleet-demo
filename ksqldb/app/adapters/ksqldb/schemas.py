import re
from typing import Optional

from pydantic import BaseModel, Field, field_validator

_SAFE_ID = re.compile(r"^[A-Za-z0-9_-]+$")
_SAFE_GEO = re.compile(r"^[0-9A-Fa-f|]+$")


def _check_safe_id(v: Optional[str], field: str) -> Optional[str]:
    if v is None:
        return v
    if not _SAFE_ID.match(v):
        raise ValueError(f"{field} must match {_SAFE_ID.pattern}")
    return v


class GeofenceRequest(BaseModel):
    robot_id: Optional[str] = Field(None, description="The specific robot to track")
    zone_id: Optional[str] = Field(None, description="The zone ID")
    config_name: str = Field(..., min_length=1, description="Name of the configuration profile")

    @field_validator("robot_id", "zone_id", "config_name")
    @classmethod
    def _ids(cls, v: Optional[str], info):
        return _check_safe_id(v, info.field_name)


class ZoneCreate(BaseModel):
    id: str = Field(..., min_length=1, description="Unique Zone Identifier")
    geo: str = Field(..., min_length=3, description="PostGIS EWKB HEX")

    @field_validator("id")
    @classmethod
    def _id(cls, v: str):
        return _check_safe_id(v, "id")

    @field_validator("geo")
    @classmethod
    def _geo(cls, v: str):
        if not _SAFE_GEO.match(v):
            raise ValueError("geo must be hex (optional '|' separators)")
        return v


class RobotCreate(BaseModel):
    id: str = Field(..., min_length=1, description="Unique Robot ID")

    @field_validator("id")
    @classmethod
    def _id(cls, v: str):
        return _check_safe_id(v, "id")


class SensorCreate(BaseModel):
    sensor_id: str = Field(..., min_length=1, description="ID of the sensor")

    @field_validator("sensor_id")
    @classmethod
    def _id(cls, v: str):
        return _check_safe_id(v, "sensor_id")


class HumidityRuleRequest(BaseModel):
    sensor_id: str = Field(..., min_length=1, description="ID of the sensor")
    min_humidity: float = Field(..., ge=0, le=100, description="Minimum humidity threshold (0-100%)")
    alert_radius_m: float = Field(..., gt=0, description="Alert radius in meters")
    config_name: str = Field(..., description="Configuration name for grouping rules")

    @field_validator("sensor_id", "config_name")
    @classmethod
    def _ids(cls, v: str, info):
        return _check_safe_id(v, info.field_name)
