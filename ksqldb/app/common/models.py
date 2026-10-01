from pydantic import BaseModel
from typing import Optional

# Common models for all adapters

class GPSPoint(BaseModel):
    """Single GPS point"""
    timestamp: float
    latitude: float
    longitude: float

class RobotBase(BaseModel):
    """Base robot model"""
    id: str
    status: Optional[str] = "ACTIVE"

class ZoneBase(BaseModel):
    """Base zone model"""
    id: str
    geo: str  # WKB HEX format
    name: Optional[str] = None