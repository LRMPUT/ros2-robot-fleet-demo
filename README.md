# ROS 2 Robot Fleet Demo

Simulate a fleet of N robots publishing sensor data to **Apache Kafka** or **MQTT**.
Each robot replays a ROS 2 bag and has its own private sink (edge topology).

## Prerequisites

- Docker Engine ≥ 24 with Compose v2
- A ROS 2 bag directory (see conversion below if you have a ROS 1 `.bag`)

### Convert a ROS 1 bag (one-time, uses Docker)

```bash
./convert_bag.sh my_recording.bag
# → bags/my_recording_ros2/
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

| Transport | Pattern | Example |
|-----------|---------|---------|
| Kafka | `ros2.robot_<id>.<suffix>` | `ros2.robot_3.gnss` |
| MQTT  | `ros2/robot_<id>/<suffix>` | `ros2/robot_3/gnss` |

| Suffix   | ROS 2 type                       | Rate    |
|----------|----------------------------------|---------|
| `gnss`   | `sensor_msgs/msg/NavSatFix`      | 10 Hz   |
| `odom`   | `nav_msgs/msg/Odometry`          | 20 Hz   |
| `scan`   | `sensor_msgs/msg/LaserScan`      | 50 Hz   |
| `points` | `sensor_msgs/msg/PointCloud2`    | 12.5 Hz |

Payloads are **CDR-serialized** ROS 2 messages. The `header.stamp` field
carries the publish wall-clock time (`t0_ns = sec×10⁹ + nanosec`).

## Demo consumer

**Option A — Docker (no local ROS 2 needed):**

```bash
# Build once
docker build -t ros2-fleet-consumer consumer/

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

## Options

| Variable | Default | Description |
|----------|---------|-------------|
| `N` | 10 | Number of robots |
| `BROKER` | kafka | `kafka` or `mqtt` |
| `BAG_PATH` | — | Path to ROS 2 bag directory |
| `MSG_TYPE` | multi | `multi` \| `navsatfix` \| `odometry` \| `laserscan` \| `pointcloud2` |
| `RATE_HZ` | 10 | Bag replay rate multiplier |
| `KAFKA_PARTITIONS` | 4 | Kafka partitions per topic (Kafka only) |

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

**Available at:** `rorbots_follower_leader_parcelle_1MONT.bag` (~345 MB)

```bash
# 1. Convert (one-time, ~2 min)
./convert_bag.sh rorbots_follower_leader_parcelle_1MONT.bag
# → bags/rorbots_follower_leader_parcelle_1MONT_ros2/

# 2. Run 10 robots → Kafka
./examples/inrae_field_campaign.sh kafka 10

# 3. Live consumer (second terminal)
cd consumer && python consume.py --broker kafka --stats-only
```

The bag contains NavSatFix, Odometry, LaserScan and PointCloud2 topics.
`robot_replay.py` selects topics **by message type**, not by name, so it
works with any bag that has those four ROS 2 types.
Each simulated robot gets its GPS position shifted by ~11 m (Δlat/Δlon = 1e-4 × robot\_id)
so the fleet appears spatially distributed across the field.

## Decoding CDR in Python

```python
from rclpy.serialization import deserialize_message
from sensor_msgs.msg import NavSatFix

# raw_bytes comes from Kafka/MQTT payload
msg = deserialize_message(raw_bytes, NavSatFix)
print(f"lat={msg.latitude:.6f}  lon={msg.longitude:.6f}  alt={msg.altitude:.1f}")
t0_ns = msg.header.stamp.sec * 1_000_000_000 + msg.header.stamp.nanosec
```
