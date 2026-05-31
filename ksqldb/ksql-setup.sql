-- Base ksqlDB schema for the ros2-robot-fleet-demo GIS4IoRT integration.
-- Loaded automatically by ksqldb-init on first startup.
--
-- kafka_sink serializes ROS messages as JSON using rosidl introspection;
-- field names are the ROS message field names verbatim.
--
-- Kafka message key = robot_id (e.g. "robot_1"), set by kafka_key in
-- subscriptions_yaml.  ksqlDB exposes it as ROWKEY / KEY columns.

SET 'auto.offset.reset' = 'earliest';

-- ── Registry tables ──────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS robot_registry (
  robot_id VARCHAR PRIMARY KEY,
  status   VARCHAR
) WITH (
  KAFKA_TOPIC  = 'robot_registration',
  VALUE_FORMAT = 'JSON',
  PARTITIONS   = 50
);

CREATE TABLE IF NOT EXISTS sensor_registry (
  sensor_id VARCHAR PRIMARY KEY,
  status    VARCHAR
) WITH (
  KAFKA_TOPIC  = 'sensor_registration',
  VALUE_FORMAT = 'JSON',
  PARTITIONS   = 4
);

-- ── Fleet sensor streams ──────────────────────────────────────────────────────
-- Topic ros2.fleet.gnss is produced by the gateway kafka_sink with:
--   kafka_name: fleet.gnss  → topic prefix ros2  → ros2.fleet.gnss
--   kafka_key:  robot_<id>  → Kafka message key
--   payload_format: json    → VALUE_FORMAT = JSON below
CREATE STREAM IF NOT EXISTS fleet_gnss_raw (
  robot_id   VARCHAR KEY,
  header     STRUCT<
               stamp    STRUCT<sec BIGINT, nanosec BIGINT>,
               frame_id VARCHAR
             >,
  status     STRUCT<status INT, service INT>,
  latitude   DOUBLE,
  longitude  DOUBLE,
  altitude   DOUBLE,
  position_covariance      ARRAY<DOUBLE>,
  position_covariance_type INT,
  `_ts`      STRUCT<t0_ns BIGINT, t1_ns BIGINT>
) WITH (
  KAFKA_TOPIC  = 'ros2.fleet.gnss',
  VALUE_FORMAT = 'JSON',
  PARTITIONS   = 50
);

-- ── Robot gps stream ──────────────────────────────────────────────────────
-- DISABLED: the geofence query reads fleet_gnss_raw directly
-- and derives the timestamp inline, so this intermediate stream is unused.
-- Re-enable only to restore the two-hop pipeline.
-- CREATE STREAM IF NOT EXISTS ROS_GPS_FIX_STREAM WITH (
--   KAFKA_TOPIC  = 'ROS_GPS_FIX_STREAM',
--   VALUE_FORMAT = 'JSON',
--   PARTITIONS   = 50
-- ) AS SELECT
--   robot_id,
--   ROWTIME                       AS event_ms,
--   latitude,
--   longitude,
--   altitude,
--   status->status                AS gps_status,
--   (header->stamp->sec * 1000)
--     + (header->stamp->nanosec / 1000000) AS timestamp
-- FROM fleet_gnss_raw
-- EMIT CHANGES;

-- translation for GIS4Io4T-kafka compatibility
--CREATE STREAM IF NOT EXISTS ROS_GPS_FIX_STREAM AS
--  SELECT
--    robot_id,
--    latitude,
--    longitude,
--    altitude,
--    t0_ms AS timestamp
--  FROM fleet_gnss
--  EMIT CHANGES;
