from fastapi import APIRouter
from app.adapters.ksqldb import database
from app.adapters.ksqldb.schemas import ZoneCreate

router = APIRouter()

@router.post("/zones", tags=["Management"])
def add_zone(zone: ZoneCreate):
    database.upsert_zone(database.ZoneEntry(zone.id, zone.geo))
    return {"status": "created", "id": zone.id}

@router.get("/zones", tags=["Management"])
def list_zones():
    return database.get_all_zones()