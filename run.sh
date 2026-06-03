#!/usr/bin/env bash
# Start a fleet of N simulated robots publishing sensor data to Kafka or MQTT.
#
# Each robot is isolated: its own ROS domain, its own sink instance.
# With fleet routing (default for Kafka), all robots write to a single shared
# Kafka topic (ros2.fleet.<suffix>) keyed by robot_id, so one ksqlDB query
# covers the whole fleet:
#
#   robot_1 (domain 1) → own sink → ros2.fleet.gnss  key=robot_1 ─┐
#   robot_2 (domain 2) → own sink → ros2.fleet.gnss  key=robot_2 ─┤ → ksqlDB
#   robot_N (domain N) → own sink → ros2.fleet.gnss  key=robot_N ─┘
#
# Required env:
#   BAG_PATH      — path to a ROS 2 bag directory (must contain metadata.yaml)
#
# Optional env:
#   N             — number of robots (default: 10)
#   BROKER        — kafka (default) | mqtt
#   MSG_TYPE      — multi (default) | navsatfix | odometry | laserscan | pointcloud2
#   RATE_HZ       — replay rate multiplier (default: 10)
#   FLEET_ROUTING — 1 (default for kafka) | 0 for per-robot topics
#
# Usage:
#   N=10 BROKER=kafka BAG_PATH=/path/to/bag ./run.sh
#   N=5  BROKER=mqtt  BAG_PATH=/path/to/bag ./run.sh
#   ./run.sh --stop           # tear down all fleet containers
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

N="${N:-10}"
BROKER="${BROKER:-kafka}"
MSG_TYPE="${MSG_TYPE:-navsatfix}"
RATE_HZ="${RATE_HZ:-10}"
if [[ "${BROKER}" == "mqtt" ]]; then
    FLEET_ROUTING="${FLEET_ROUTING:-0}"
else
    FLEET_ROUTING="${FLEET_ROUTING:-1}"
fi

# Store fleet compose files persistently so --stop always finds them.
FLEET_DIR="${SCRIPT_DIR}/.fleet"
mkdir -p "${FLEET_DIR}"
FLEET_COMPOSE="${FLEET_DIR}/robots_${BROKER}_${N}.yml"
COMPOSE_ARGS=(-f "docker-compose.${BROKER}.yml" -f "${FLEET_COMPOSE}")

stop_fleet() {
    echo "[fleet] tearing down..."
    if [[ -f "${FLEET_COMPOSE}" ]]; then
        BAG_PATH=/dev/null N="${N}" \
            docker compose "${COMPOSE_ARGS[@]}" down -v --remove-orphans 2>&1 | tail -3 || true
        rm -f "${FLEET_COMPOSE}"
    else
        docker compose -f "docker-compose.${BROKER}.yml" down -v --remove-orphans 2>&1 | tail -3 || true
    fi
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
export BAG_PATH MSG_TYPE RATE_HZ FLEET_ROUTING

echo "============================================="
echo "  ROS 2 Robot Fleet"
echo "  Robots : ${N}"
echo "  Broker : ${BROKER}"
echo "  Topics : ${MSG_TYPE}"
echo "  Fleet  : $([ "${FLEET_ROUTING}" == "1" ] && echo "shared topic (ros2.fleet.*)" || echo "per-robot topics")"
echo "  Bag    : ${BAG_PATH}"
echo "============================================="

# 1. Generate per-robot compose fragment.
BROKER="${BROKER}" MSG_TYPE="${MSG_TYPE}" RATE_HZ="${RATE_HZ}" FLEET_ROUTING="${FLEET_ROUTING}" \
    "${SCRIPT_DIR}/gen_fleet.sh" "${N}" "${FLEET_COMPOSE}"

# 2. Start broker.
echo "[fleet] starting ${BROKER} broker..."
N="${N}" docker compose "${COMPOSE_ARGS[@]}" up -d broker
sleep 6

# 3. Start robots in batches to avoid DDS multicast burst on host network.
echo "[fleet] starting ${N} robots..."
BATCH=5
for ((i=1; i<=N; i+=BATCH)); do
    batch_services=""
    for ((j=i; j<i+BATCH && j<=N; j++)); do
        batch_services+="robot_${j} "
    done
    N="${N}" MSG_TYPE="${MSG_TYPE}" \
        docker compose "${COMPOSE_ARGS[@]}" up -d --no-deps ${batch_services}
    [[ $((i + BATCH)) -le N ]] && sleep 3
done

bringup_pad=$(( 15 + N / 2 ))
echo "[fleet] waiting ${bringup_pad}s for lifecycle init..."
sleep "${bringup_pad}"

echo ""
echo "Fleet is running."
case "${BROKER}" in
    kafka)
        if [[ "${FLEET_ROUTING}" == "1" ]]; then
            echo "  Bootstrap : localhost:9092"
            echo "  Topics    : ros2.fleet.gnss | ros2.fleet.odom | ros2.fleet.scan | ros2.fleet.points"
            echo "  Key       : robot_<id> (identifies the source robot)"
        else
            echo "  Bootstrap : localhost:9092"
            echo "  Topics    : ros2.robot_<id>.gnss | .odom | .scan | .points"
        fi
        echo ""
        echo "  Quick check:"
        echo "    docker run --rm --network host confluentinc/cp-kafka:latest \\"
        echo "      kafka-topics --bootstrap-server localhost:9092 --list"
        ;;
    mqtt)
        echo "  Broker    : localhost:1883"
        echo "  Topics    : ros2/robot_<id>/gnss | /odom | /scan | /points"
        echo ""
        echo "  Quick check:"
        echo "    mosquitto_sub -h localhost -p 1883 -t 'ros2/#' -v"
        ;;
esac
echo ""
echo "  To stop: ./run.sh --stop"
