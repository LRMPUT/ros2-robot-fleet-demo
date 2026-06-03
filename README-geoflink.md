
## Apache Flink + GeoFlink analytics

The optional `docker-compose.geoflink.yml` overlay adds a real-time analytics layer on
top of the Kafka fleet stack: **Apache Flink**, **Jar Uploader**, **Kafka UI**, and the
**GIS4IoRT API** (geofencing + collision detection).

The FastAPI app, GIS UDF jar and benchmarking all live in the
[`GIS4IoRT-geoflink`](https://github.com/Frostyyyl/GIS4IoRT-geoflink) submodule
(third-party, pinned). Our overlay just adds fleet-specific schema on top.

### Architecture

```
robot_1 (domain 1) → kafka_sink → ros2.fleet.gnss  key=robot_1 ─┐
robot_2 (domain 2) → kafka_sink → ros2.fleet.gnss  key=robot_2 ─┤ → Apache Flink → GIS API
robot_N (domain N) → kafka_sink → ros2.fleet.gnss  key=robot_N ─┘
```

All robot sinks write JSON to the **same** Kafka topic; Apache Flink consumes it, processes it and exposes the results to the GIS4IoRT API.  No extra bridge or CDR→JSON converter is
needed — the fleet stack already defaults to `PAYLOAD_FORMAT=json` for Kafka.

### 1. Initialize the GIS4IoRT submodule

```bash
# One-time after cloning
git submodule update --init --recursive
```

<!-- This populates `GIS4IoRT-geoflink/` from the upstream repo. The overlay compose builds
the GIS API container from `GIS4IoRT-ksqlDB/deployments/ksqldb/Dockerfile.api` and
mounts the UDF jar from `GIS4IoRT-ksqlDB/deployments/ksqldb/udf/build/libs/`. -->

To pull upstream changes later:
```bash
git submodule update --remote GIS4IoRT-geoflink     # fetch latest upstream commit
git diff GIS4IoRT-geoflink                          # review the bump
# rebuild & smoke-test the stack
git add GIS4IoRT-geoflink && git commit -m "bump GIS4IoRT-geoflink"
```

### 2. Start the analytics stack

```bash
# Start geoflink + GIS API alongside the Kafka broker (do this first, before the fleet)
BAG_PATH=/home/purple-panda/Projects/ros2-robot-fleet-demo/bags/rorbots_follower_leader_parcelle_1MONT_ros2 \
docker compose -f docker-compose.kafka.yml \
               -f docker-compose.geoflink.yml \
               up -d
```