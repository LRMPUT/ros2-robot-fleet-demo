-- Base ksqlDB schema for the ros2-robot-fleet-demo GIS4IoRT integration.
-- Loaded automatically by ksqldb-init on first startup.
--
-- Fleet sensor streams (ros2.fleet.gnss.json etc.) are added in step 3
-- once the CDR-to-JSON bridge is wired up.

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
