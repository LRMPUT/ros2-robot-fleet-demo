# ROS 2 Robot Fleet Demo

Simulate a fleet of N robots publishing sensor data to **Apache Kafka** or **MQTT**.
Each robot replays a ROS 2 bag and has its own private sink (edge topology).

![10-robot GNSS trajectories on satellite map](docs/trajectories.png)

## Prerequisites

- Docker Engine ≥ 24 with Compose v2
- A ROS 2 bag directory (see conversion below if you have a ROS 1 `.bag`)

### Convert a ROS 1 bag (one-time)

**Option A — Docker (no local Python needed):**

```bash
./convert_bag.sh my_recording.bag
# → bags/my_recording_ros2/
```

**Option B — manual (requires `pip install rosbags`):**

```bash
pip install rosbags
rosbags-convert \
    --src my_recording.bag \
    --dst bags/my_recording_ros2 \
    --dst-version 8 --dst-typestore ros2_humble
```

## Quickstart

```bash
# 10 robots → Kafka
N=10 BROKER=kafka BAG_PATH=/tmp/my_bag_ros2 ./run.sh

# 5 robots → MQTT
N=5 BROKER=mqtt BAG_PATH=/tmp/my_bag_ros2 ./run.sh

# Stop the fleet
./run.sh --stop
```

## Topic naming

Each robot is isolated — its own ROS domain, its own sink instance.
By default (Kafka, `FLEET_ROUTING=1`) all robots write to **one shared topic**
keyed by `robot_id`, enabling a single ksqlDB query for the whole fleet:

```
robot_1 (domain 1) → own sink → ros2.fleet.gnss  key=robot_1 ─┐
robot_2 (domain 2) → own sink → ros2.fleet.gnss  key=robot_2 ─┤ → ksqlDB
robot_N (domain N) → own sink → ros2.fleet.gnss  key=robot_N ─┘
```

| Transport | Routing | Topic pattern | Example |
|-----------|---------|---------------|---------|
| Kafka (default) | fleet | `ros2.fleet.<suffix>` | `ros2.fleet.gnss` |
| Kafka | per-robot (`FLEET_ROUTING=0`) | `ros2.robot_<id>.<suffix>` | `ros2.robot_3.gnss` |
| MQTT  | per-robot | `ros2/robot_<id>/<suffix>` | `ros2/robot_3/gnss` |

| Suffix   | ROS 2 type                       | Rate    |
|----------|----------------------------------|---------|
| `gnss`   | `sensor_msgs/msg/NavSatFix`      | 10 Hz   |
| `odom`   | `nav_msgs/msg/Odometry`          | 20 Hz   |
| `scan`   | `sensor_msgs/msg/LaserScan`      | 50 Hz   |
| `points` | `sensor_msgs/msg/PointCloud2`    | 12.5 Hz |

Kafka payloads are **JSON-serialized** ROS 2 messages (field names match
the ROS message definition). The `header.stamp` field carries the
publish wall-clock time (`t0_ns = sec×10⁹ + nanosec`).
MQTT payloads are CDR-serialized by default.

## Demo consumer

**Option A — Docker (no local ROS 2 needed):**

```bash
# Build once
docker build -t ros2-fleet-consumer -f consumer/Dockerfile .

# Kafka
docker run --rm --network host ros2-fleet-consumer --broker kafka

# MQTT
docker run --rm --network host ros2-fleet-consumer --broker mqtt

# Stats only
docker run --rm --network host ros2-fleet-consumer --broker kafka --stats-only

# Specific robots
docker run --rm --network host ros2-fleet-consumer --broker kafka --robots 1,3,5
```

**Option B — local Python (requires ROS 2 Humble):**

```bash
cd consumer
pip install -r requirements.txt

python consume.py --broker kafka
python consume.py --broker mqtt --stats-only
```

Output example:
```
Listening on KAFKA... (Ctrl+C to stop)

[robot_  1]  gnss      1.2 ms    0.1 KB  ← ros2.robot_1.gnss
[robot_  2]  scan      0.9 ms   28.0 KB  ← ros2.robot_2.scan
[robot_  1]  odom      1.1 ms    0.3 KB  ← ros2.robot_1.odom
...
[stats]     925 msg/s   11340.2 KB/s  robots=10  total=55,500
```

## Images

Images are published to GHCR from the
[ros2_kafka_dispatcher](https://github.com/LRMPUT/ros2_kafka_dispatcher) repo:

| Image | Purpose |
|-------|---------|
| `ghcr.io/lrmput/ros2-kafka-dispatcher:latest` | Dispatcher (kafka_sink / mosquitto_sink) + robot publisher |

### Build locally (alternative)

```bash
# From the ros2_kafka_dispatcher repo root:
docker build -f docker/Dockerfile --build-arg ROS_DISTRO=humble \
    -t ghcr.io/lrmput/ros2-kafka-dispatcher:latest .
```

## Using the INRAE field campaign bag

The demo was developed using a real ROS 1 bag from a leader/follower
agricultural campaign at INRAE Clermont-Ferrand.

**Download:** [Google Drive (~345 MB)](https://drive.google.com/drive/folders/1ZtEteOZKS7RpE3ClVaJJBrmUmUiiYYma?usp=sharing)

```bash
# 1. Convert (one-time, ~2 min)
./convert_bag.sh rorbots_follower_leader_parcelle_1MONT.bag
# → bags/rorbots_follower_leader_parcelle_1MONT_ros2/

# 2. Run 10 robots → Kafka
./examples/inrae_field_campaign.sh kafka 10

# 3. Live consumer (second terminal)
docker run --rm --network host ros2-fleet-consumer --broker kafka --stats-only
```

The bag contains NavSatFix, Odometry, LaserScan and PointCloud2 topics.
`robot_replay.py` selects topics **by message type**, not by name, so it
works with any bag that has those four ROS 2 types.
Each simulated robot gets its GPS position shifted **~6 m perpendicular to the field's
travel direction** (computed via PCA on the bag trajectory), so the 10 robots form
non-crossing parallel tracks centred around the original route.

## Trajectory recording and plotting

Record GNSS data flowing through the live Kafka or MQTT pipeline, then plot
each robot's path on a satellite map.

### 1. Start a fleet

```bash
N=10 BROKER=mqtt BAG_PATH=/path/to/bag ./run.sh
# or
N=10 BROKER=kafka BAG_PATH=/path/to/bag ./run.sh
```

### 2. Record GNSS from the pipeline

```bash
pip install pandas geopandas folium contextily matplotlib paho-mqtt confluent-kafka

# MQTT fleet
python3 tools/record_gnss.py --broker mqtt --robots 1-10 --out trajectories/ --duration 120

# Kafka fleet
python3 tools/record_gnss.py --broker kafka --robots 1-10 --out trajectories/ --duration 120
```

Writes one `robot_N_gnss.txt` file per robot (`timestamp_ns / latitude / longitude / altitude`).
Auto-detects JSON and CDR payloads from the broker headers.

### 3. Plot trajectories

```bash
python3 tools/plot_trajectories.py trajectories/
```

Outputs:
- `trajectories.html` — interactive Leaflet map (open in browser)
- `trajectories.png` — 500 dpi satellite map (Esri WorldImagery)
- `trajectories.pdf` — vector version for publications

## ksqlDB analytics

The optional `docker-compose.ksqldb.yml` overlay adds a real-time analytics layer on
top of the Kafka fleet stack: **ksqlDB**, **Schema Registry**, **Kafka UI**, and the
**GIS4IoRT API** (geofencing + collision detection).

The FastAPI app, `Dockerfile.api`, and GIS UDF jar all live in the
[`GIS4IoRT-ksqlDB`](https://github.com/AntoniSopata/GIS4IoRT-ksqlDB) submodule
(third-party, pinned). Our overlay just adds fleet-specific schema on top.

### Architecture

```
robot_1 (domain 1) → kafka_sink → ros2.fleet.gnss  key=robot_1 ─┐
robot_2 (domain 2) → kafka_sink → ros2.fleet.gnss  key=robot_2 ─┤ → ksqlDB → GIS API
robot_N (domain N) → kafka_sink → ros2.fleet.gnss  key=robot_N ─┘
```

All robot sinks write JSON to the **same** Kafka topic; ksqlDB consumes it as a single
stream and exposes it to the GIS4IoRT API.  No extra bridge or CDR→JSON converter is
needed — the fleet stack already defaults to `PAYLOAD_FORMAT=json` for Kafka.

### 1. Initialize the GIS4IoRT submodule

```bash
# One-time after cloning
git submodule update --init --recursive
```

This populates `GIS4IoRT-ksqlDB/` from the upstream repo. The overlay compose builds
the GIS API container from `GIS4IoRT-ksqlDB/deployments/ksqldb/Dockerfile.api` and
mounts the UDF jar from `GIS4IoRT-ksqlDB/deployments/ksqldb/udf/build/libs/`.

To pull upstream changes later:
```bash
git submodule update --remote GIS4IoRT-ksqlDB     # fetch latest upstream commit
git diff GIS4IoRT-ksqlDB                          # review the bump
# rebuild & smoke-test the stack
git add GIS4IoRT-ksqlDB && git commit -m "bump GIS4IoRT-ksqlDB"
```

### 2. Start the analytics stack

```bash
# Start ksqlDB + GIS API alongside the Kafka broker (do this first, before the fleet)
BAG_PATH=/path/to/bag \
  docker compose -f docker-compose.kafka.yml \
                 -f docker-compose.ksqldb.yml \
                 up -d
```

Wait ~30 s for ksqlDB to become healthy and for `ksqldb-init` to load the schema.

### 3. Start the robot fleet

```bash
# Fleet routing is on by default for Kafka (FLEET_ROUTING=1)
N=10 BROKER=kafka BAG_PATH=/path/to/bag ./run.sh
```

Each robot's sink will publish to `ros2.fleet.gnss` (and `odom`, `scan`, `points`)
with `kafka_key=robot_<id>` so ksqlDB can identify the source robot.

### 4. Query live GNSS data

```bash
# Open the ksqlDB CLI
docker exec -it $(docker ps -qf name=ksqldb-server) ksql http://localhost:8088

# Stream live positions from all robots
ksql> SELECT robot_id, latitude, longitude, altitude FROM FLEET_GNSS EMIT CHANGES;

# Latest fix per robot (table scan)
ksql> SELECT robot_id, latitude, longitude FROM FLEET_GNSS EMIT CHANGES LIMIT 10;
```

### Services

| Service | URL | Description |
|---------|-----|-------------|
| ksqlDB REST | http://localhost:8088 | Query endpoint (ksql CLI / REST API) |
| Kafka UI | http://localhost:8090 | Browse topics, messages, consumer groups |
| GIS4IoRT API | http://localhost:8000/docs | Swagger UI — geofencing, WebSocket streams |
| Schema Registry | http://localhost:8081 | Confluent Schema Registry |

### Stop

```bash
# Stop ksqlDB overlay only
docker compose -f docker-compose.kafka.yml -f docker-compose.ksqldb.yml down

# Stop everything (fleet + analytics)
./run.sh --stop
docker compose -f docker-compose.kafka.yml -f docker-compose.ksqldb.yml down
```

## Decoding CDR in Python

```python
from rclpy.serialization import deserialize_message
from sensor_msgs.msg import NavSatFix

# raw_bytes comes from Kafka/MQTT payload
msg = deserialize_message(raw_bytes, NavSatFix)
print(f"lat={msg.latitude:.6f}  lon={msg.longitude:.6f}  alt={msg.altitude:.1f}")
t0_ns = msg.header.stamp.sec * 1_000_000_000 + msg.header.stamp.nanosec
```
