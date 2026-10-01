from fastapi import FastAPI
from contextlib import asynccontextmanager, AsyncExitStack
import logging
import os

logger = logging.getLogger("uvicorn.info")

ACTIVE_ADAPTER = os.getenv("ACTIVE_ADAPTER", "geoflink")  # Default: geoflink

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with AsyncExitStack() as stack:
        
        if ACTIVE_ADAPTER == "geoflink":
            logger.info("Starting GeoFlink adapter...")
            from app.adapters.geoflink import database as geoflink_database
            from app.adapters.geoflink.lifecycle import geoflink_lifespan
            geoflink_database.init_db()
            await stack.enter_async_context(geoflink_lifespan(app))
        
        elif ACTIVE_ADAPTER == "ksqldb":
            logger.info("Starting ksqlDB adapter...")
            from app.adapters.ksqldb import database as ksqldb_database
            from app.adapters.ksqldb.kafka_service import kafka_service as ksqldb_kafka_service
            from app.adapters.ksqldb.consumer_manager import consumer_manager as ksqldb_consumer_manager
            
            
            ksqldb_database.init_db()
            await ksqldb_kafka_service.start()
            
        elif ACTIVE_ADAPTER == "nebulastream":
            logger.info("Starting NebulaStream adapter...")
            # TODO: NebulaStream lifecycle
            pass
        
        yield
        
        # Cleanup
        if ACTIVE_ADAPTER == "ksqldb":
            from app.adapters.ksqldb.kafka_service import kafka_service as ksqldb_kafka_service
            from app.adapters.ksqldb.consumer_manager import consumer_manager as ksqldb_consumer_manager
            
            await ksqldb_consumer_manager.stop_all()
            await ksqldb_kafka_service.stop()

app = FastAPI(
    title="GIS4IoRT Processing Layer API",
    description=f"Multi-adapter streaming processing API (Active: {ACTIVE_ADAPTER})",
    lifespan=lifespan
)


# Register routers based on active adapter
if ACTIVE_ADAPTER == "geoflink":
    from app.adapters.geoflink.routers import router as geoflink_router
    app.include_router(geoflink_router, prefix="/geoflink")

elif ACTIVE_ADAPTER == "ksqldb":
    from app.adapters.ksqldb.routers import (
        robots as ksqldb_robots,
        zones as ksqldb_zones,
        geofence as ksqldb_geofence,
        speed as ksqldb_speed,
        historical as ksqldb_historical,
        websockets as ksqldb_websockets,
        humidity as ksqldb_humidity,
        collision as ksqldb_collision
    )
    
    app.include_router(ksqldb_robots.router, prefix="/ksqldb")
    app.include_router(ksqldb_zones.router, prefix="/ksqldb")
    app.include_router(ksqldb_geofence.router, prefix="/ksqldb")
    app.include_router(ksqldb_speed.router, prefix="/ksqldb")
    app.include_router(ksqldb_historical.router, prefix="/ksqldb")
    app.include_router(ksqldb_websockets.router, prefix="/ksqldb")
    app.include_router(ksqldb_humidity.router, prefix="/ksqldb")
    app.include_router(ksqldb_collision.router, prefix="/ksqldb")

elif ACTIVE_ADAPTER == "nebulastream":
    # TODO: NebulaStream routers
    pass

@app.get("/", tags=["Health"])
def root():
    return {
        "system": "GIS4IoRT Processing Layer",
        "active_adapter": ACTIVE_ADAPTER,
        "status": "RUNNING",
        "docs": "/docs"
    }

@app.get("/health", tags=["Health"])
def health():
    return {
        "adapter": ACTIVE_ADAPTER,
        "status": "healthy"
    }