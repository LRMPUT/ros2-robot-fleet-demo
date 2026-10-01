from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

class Settings(BaseSettings):
    # Common settings
    ACTIVE_ADAPTER: str = "geoflink"  # geoflink | ksqldb | nebulastream
    
    # geoflink
    FLINK_URL: str = "http://localhost:8082"
    KAFKA_BROKER: str = "127.0.0.1:9092"
    FLINK_JAR_NAME: str = "GeoFlinkProject-0.1"
    GEOFLINK_DB_NAME: str = "geoflink_registry.db"
    
    # ksqldb
    KAFKA_BOOTSTRAP_SERVERS: str = "broker:29092"
    KSQLDB_DB_NAME: str = "gis4iort.db"
    ROSBAG_PATH: str = "/bags/rorbots_follower_leader_parcelle_1MONT_ros2"
    
    # nebulastream
    # TODO: Add NebulaStream config
    
    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore"  # ignore unknown env parameters
    )

settings = Settings()