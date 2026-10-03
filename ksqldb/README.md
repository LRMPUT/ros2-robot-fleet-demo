# ksqlDB / GIS overlay (fleet demo)

**Source of truth for this repo’s compose overlay** (`docker-compose.ksqldb.yml`).

| Path | Role |
|------|------|
| `ksqldb/` | Fleet-demo API, `ksql-setup.sql`, `fleet_bridge`, UDF jar — **use this** |
| `GIS4IoRT-ksqlDB/` | Nested upstream checkout (own gitdir). May drift; do not edit for fleet fixes unless syncing back upstream |

## Data path

1. Start Kafka fleet with **`PAYLOAD_FORMAT=json`**.
2. `fleet-bridge` flattens `ros2.robot_<id>.{gnss,odom}` → `ros2.fleet.{gnss,odom}`.
3. `ksql-setup.sql` defines `ROS_GPS_FIX_STREAM` / `ROS_FILTERED_ODOM_STREAM` on those topics.
4. Mutating API routes (`/ksqldb/geofence`, `/speed`, `/humidity`) create derived alert queries.

Lab ports are bound to `127.0.0.1` only. Mutating IDs must match `^[A-Za-z0-9_-]+$`.
