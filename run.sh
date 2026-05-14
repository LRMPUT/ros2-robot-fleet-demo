#!/usr/bin/env bash
# Start a fleet of N simulated robots publishing sensor data to Kafka or MQTT.
#
# Each robot replays a ROS 2 bag and has its own private sink (edge topology):
#   NavSatFix  @ 10 Hz  → ros2.robot_<id>.gnss  (Kafka) / ros2/robot_<id>/gnss  (MQTT)
#   Odometry   @ 20 Hz  → ros2.robot_<id>.odom
#   LaserScan  @ 50 Hz  → ros2.robot_<id>.scan
#   PointCloud2@ 12.5Hz → ros2.robot_<id>.points
#
# Required env:
#   BAG_PATH   — path to a converted ROS 2 bag directory (must contain metadata.yaml)
#
# Optional env:
#   N          — number of robots (default: 10)
#   BROKER     — kafka (default) or mqtt
#   MSG_TYPE   — multi (default) | navsatfix | odometry | laserscan | pointcloud2
#   RATE_HZ    — replay rate multiplier (default: 10)
#
# Usage:
#   N=10 BROKER=kafka BAG_PATH=/path/to/bag ./run.sh
#   N=5  BROKER=mqtt  BAG_PATH=/path/to/bag ./run.sh
#   ./run.sh --stop      # tear down everything
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

N="${N:-10}"
BROKER="${BROKER:-kafka}"
MSG_TYPE="${MSG_TYPE:-multi}"
RATE_HZ="${RATE_HZ:-10}"

FLEET_COMPOSE="/tmp/fleet_robots_${BROKER}_${N}.yml"
COMPOSE_ARGS=(-f "docker-compose.${BROKER}.yml" -f "${FLEET_COMPOSE}")

stop_fleet() {
    echo "[fleet] tearing down..."
    NUM_ROBOTS="${N}" docker compose "${COMPOSE_ARGS[@]}" down -v --remove-orphans 2>&1 | tail -3 || true
    rm -f "${FLEET_COMPOSE}"
}

if [[ "${1:-}" == "--stop" ]]; then
    stop_fleet
    exit 0
fi

: "${BAG_PATH:?BAG_PATH env var is required (path to ROS 2 bag directory)}"
if [[ ! -f "${BAG_PATH}/metadata.yaml" ]]; then
    echo "ERROR: ${BAG_PATH}/metadata.yaml not found — BAG_PATH must point to a ROS 2 bag directory." >&2
    exit 2
fi
export BAG_PATH MSG_TYPE RATE_HZ

echo "============================================="
echo "  ROS 2 Robot Fleet"
echo "  Robots : ${N}"
echo "  Broker : ${BROKER}"
echo "  Topics : ${MSG_TYPE}"
echo "  Bag    : ${BAG_PATH}"
echo "============================================="

# 1. Generate per-robot compose fragment.
BROKER="${BROKER}" MSG_TYPE="${MSG_TYPE}" RATE_HZ="${RATE_HZ}" \
    "${SCRIPT_DIR}/gen_fleet.sh" "${N}" "${FLEET_COMPOSE}"

# 2. Start broker.
echo "[fleet] starting ${BROKER} broker..."
NUM_ROBOTS="${N}" docker compose "${COMPOSE_ARGS[@]}" up -d broker
sleep 6

# 3. Start robots (parallel lifecycle init + bringup pad).
echo "[fleet] starting ${N} robots..."
robot_services=""
for ((i=1; i<=N; i++)); do robot_services+="robot_${i} "; done

NUM_ROBOTS="${N}" MSG_TYPE="${MSG_TYPE}" \
    docker compose "${COMPOSE_ARGS[@]}" up -d --no-deps ${robot_services}

bringup_pad=$(( 8 + N / 2 ))
echo "[fleet] waiting ${bringup_pad}s for lifecycle init..."
sleep "${bringup_pad}"

echo ""
echo "Fleet is running. Topics available:"
case "${BROKER}" in
    kafka)
        echo "  Bootstrap : localhost:9092"
        echo "  Topics    : ros2.robot_<id>.gnss | .odom | .scan | .points"
        echo ""
        echo "  Quick check:"
        echo "    docker run --rm --network host confluentinc/cp-kafka:latest \\"
        echo "      kafka-topics --bootstrap-server localhost:9092 --list"
        echo ""
        echo "  Demo consumer (live print):"
        echo "    cd consumer && pip install -r requirements.txt"
        echo "    python consume.py --broker kafka"
        ;;
    mqtt)
        echo "  Broker    : localhost:1883"
        echo "  Topics    : ros2/robot_<id>/gnss | /odom | /scan | /points"
        echo ""
        echo "  Quick check (mosquitto_sub):"
        echo "    mosquitto_sub -h localhost -p 1883 -t 'ros2/#' -v"
        echo ""
        echo "  Demo consumer (live print):"
        echo "    cd consumer && pip install -r requirements.txt"
        echo "    python consume.py --broker mqtt"
        ;;
esac
echo ""
echo "  To stop: ./run.sh --stop"
echo "        or: N=${N} BROKER=${BROKER} BAG_PATH=${BAG_PATH} ./run.sh --stop"
