-- Base ksqlDB schema for the ros2-robot-fleet-demo GIS4IoRT integration.
-- Loaded automatically by ksqldb-init on first start.
--
-- Fleet path (PAYLOAD_FORMAT=json):
--   robot containers → ros2.robot_<id>.{gnss,odom}
--   fleet-bridge     → ros2.fleet.{gnss,odom}   (flattened)
--   streams below    → ROS_GPS_FIX_STREAM / ROS_FILTERED_ODOM_STREAM

SET 'auto.offset.reset' = 'earliest';

CREATE TABLE IF NOT EXISTS robot_registry (
  robot_id VARCHAR PRIMARY KEY,
  status   VARCHAR
) WITH (
  KAFKA_TOPIC   = 'robot_registration',
  VALUE_FORMAT  = 'JSON',
  PARTITIONS    = 4
);

CREATE TABLE IF NOT EXISTS sensor_registry (
  sensor_id VARCHAR PRIMARY KEY,
  status    VARCHAR
) WITH (
  KAFKA_TOPIC   = 'sensor_registration',
  VALUE_FORMAT  = 'JSON',
  PARTITIONS    = 4
);

-- Flattened GNSS (written by ksqldb/app/fleet_bridge.py).
CREATE STREAM IF NOT EXISTS ROS_GPS_FIX_STREAM (
  robot_id  VARCHAR KEY,
  timestamp BIGINT,
  latitude  DOUBLE,
  longitude DOUBLE
) WITH (
  KAFKA_TOPIC  = 'ros2.fleet.gnss',
  VALUE_FORMAT = 'JSON',
  TIMESTAMP    = 'timestamp',
  PARTITIONS   = 4
);

-- Flattened odometry (written by ksqldb/app/fleet_bridge.py).
CREATE STREAM IF NOT EXISTS ROS_FILTERED_ODOM_STREAM (
  robot_id   VARCHAR KEY,
  timestamp  BIGINT,
  position_x DOUBLE,
  position_y DOUBLE,
  position_z DOUBLE
) WITH (
  KAFKA_TOPIC  = 'ros2.fleet.odom',
  VALUE_FORMAT = 'JSON',
  TIMESTAMP    = 'timestamp',
  PARTITIONS   = 4
);
