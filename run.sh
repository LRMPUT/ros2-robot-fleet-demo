#!/usr/bin/env bash
# Start a fleet of N simulated robots publishing sensor data to Kafka or MQTT.
#
# Each robot replays a ROS 2 bag and has its own private sink (edge topology):
#   NavSatFix  @ 10 Hz  → ros2.robot_<id>.gnss  (Kafka) / ros2/robot_<id>/gnss  (MQTT)
#   Odometry   @ 20 Hz  → ros2.robot_<id>.odom
#   LaserScan  @ 50 Hz  → ros2.robot_<id>.scan
#   PointCloud2@ 12.5Hz → ros2.robot_<id>.points
#
# Required env (for robots stage):
#   BAG_PATH   — path to a converted ROS 2 bag directory (must contain metadata.yaml)
#
# Optional env:
#   N          — number of robots (default: 10)
#   BROKER     — kafka (default) or mqtt
#   MSG_TYPE   — multi (default) | navsatfix | odometry | laserscan | pointcloud2
#   RATE_HZ    — replay rate multiplier (default: 10)
#   TOPOLOGY   — shared (default) | per-robot (MQTT only: one broker per robot)
#
# Usage:
#   N=10 BROKER=kafka BAG_PATH=/path/to/bag ./run.sh
#   N=5  BROKER=mqtt  BAG_PATH=/path/to/bag ./run.sh
#   ./run.sh --stop                  # tear down all fleets
#
# Manual 3-stage startup (e.g. to start downstream consumers between brokers and robots):
#   N=5 BROKER=mqtt                              ./run.sh --stage brokers
#   # ... start your downstream consumer (nebula etc.) ...
#   N=5 BROKER=mqtt BAG_PATH=/path/to/bag         ./run.sh --stage robots
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

# Load .env if present; shell-set vars always take precedence.
if [[ -f "${SCRIPT_DIR}/.env" ]]; then
    while IFS= read -r line || [[ -n "$line" ]]; do
        [[ "$line" =~ ^[[:space:]]*# ]] && continue
        [[ "$line" =~ ^[[:space:]]*$ ]] && continue
        key="${line%%=*}"
        [[ -v "$key" ]] && continue
        export "$line"
    done < "${SCRIPT_DIR}/.env"
fi

N="${N:-10}"
BROKER="${BROKER:-kafka}"
MSG_TYPE="${MSG_TYPE:-multi}"
RATE_HZ="${RATE_HZ:-10}"
TOPOLOGY="${TOPOLOGY:-shared}"

# Store fleet compose files persistently so --stop always finds them.
FLEET_DIR="${SCRIPT_DIR}/.fleet"
mkdir -p "${FLEET_DIR}"
if [[ "${TOPOLOGY}" == "per-robot" ]]; then
    FLEET_COMPOSE="${FLEET_DIR}/robots_${BROKER}_${N}_per-robot.yml"
    COMPOSE_ARGS=(-f "${FLEET_COMPOSE}")
else
    FLEET_COMPOSE="${FLEET_DIR}/robots_${BROKER}_${N}.yml"
    COMPOSE_ARGS=(-f "docker-compose.${BROKER}.yml" -f "${FLEET_COMPOSE}")
fi

stop_fleet() {
    echo "[fleet] tearing down..."
    if [[ -f "${FLEET_COMPOSE}" ]]; then
        # BAG_PATH is required by the compose template but irrelevant for `down`.
        BAG_PATH=/dev/null NUM_ROBOTS="${N}" \
            docker compose "${COMPOSE_ARGS[@]}" down -v --remove-orphans 2>&1 | tail -3 || true
        rm -f "${FLEET_COMPOSE}"
    else
        if [[ "${TOPOLOGY}" == "per-robot" ]]; then
            docker compose -p "ros2-robot-fleet-demo" down -v --remove-orphans 2>&1 | tail -3 || true
        else
            # Fallback: tear down by project name (broker + any orphaned containers).
            docker compose -f "docker-compose.${BROKER}.yml" down -v --remove-orphans 2>&1 | tail -3 || true
        fi
    fi
}

if [[ "${1:-}" == "--stop" ]]; then
    echo "  Robots   : ${N}"
    stop_fleet
    exit 0
fi

# Parse --stage flag (default: all = brokers + robots in one go)
STAGE="all"
if [[ "${1:-}" == "--stage" ]]; then
    STAGE="${2:?--stage requires a value: brokers, robots, or all}"
    case "${STAGE}" in
        brokers|robots|all) ;;
        *) echo "ERROR: unknown --stage value '${STAGE}'; expected brokers|robots|all" >&2; exit 1 ;;
    esac
fi

if [[ "${TOPOLOGY}" == "per-robot" && "${BROKER}" != "mqtt" ]]; then
    echo "ERROR: TOPOLOGY=per-robot is only supported with BROKER=mqtt" >&2
    exit 1
fi

# BAG_PATH is only required when we'll actually start robot containers.
# For --stage brokers, set a placeholder so Docker Compose can still parse the file.
if [[ "${STAGE}" == "brokers" ]]; then
    export BAG_PATH="${BAG_PATH:-/dev/null}"
else
    : "${BAG_PATH:?BAG_PATH env var is required (path to ROS 2 bag directory)}"
    if [[ ! -f "${BAG_PATH}/metadata.yaml" ]]; then
        echo "ERROR: ${BAG_PATH}/metadata.yaml not found — BAG_PATH must point to a ROS 2 bag directory." >&2
        exit 2
    fi
    export BAG_PATH
fi
export MSG_TYPE RATE_HZ

echo "============================================="
echo "  ROS 2 Robot Fleet"
echo "  Robots   : ${N}"
echo "  Broker   : ${BROKER}"
echo "  Topology : ${TOPOLOGY}"
echo "  Topics   : ${MSG_TYPE}"
echo "  Stage    : ${STAGE}"
[[ "${STAGE}" != "brokers" ]] && echo "  Bag      : ${BAG_PATH}"
echo "============================================="

# === Stage: brokers (or full run) ===
if [[ "${STAGE}" == "brokers" || "${STAGE}" == "all" ]]; then
    # Generate compose fragment.
    BROKER="${BROKER}" MSG_TYPE="${MSG_TYPE}" RATE_HZ="${RATE_HZ}" TOPOLOGY="${TOPOLOGY}" \
        "${SCRIPT_DIR}/gen_fleet.sh" "${N}" "${FLEET_COMPOSE}"

    # Start broker(s).
    if [[ "${TOPOLOGY}" == "per-robot" ]]; then
        echo "[fleet] starting ${N} per-robot brokers..."
        broker_services=()
        for ((i=1; i<=N; i++)); do broker_services+=("broker_${i}"); done
        docker compose "${COMPOSE_ARGS[@]}" up -d "${broker_services[@]}"
    else
        echo "[fleet] starting ${BROKER} broker..."
        docker compose "${COMPOSE_ARGS[@]}" up -d broker
    fi
    sleep 6
fi

# Stop here if user only wanted brokers.
if [[ "${STAGE}" == "brokers" ]]; then
    echo ""
    echo "Brokers ready. Next:"
    case "${BROKER}" in
        mqtt)
            if [[ "${TOPOLOGY}" == "per-robot" ]]; then
                echo "  Brokers : localhost:1883 … localhost:$((1882 + N))"
            else
                echo "  Broker  : localhost:1883"
            fi
            ;;
        kafka)
            echo "  Bootstrap : localhost:9092"
            ;;
    esac
    echo ""
    echo "  Start downstream consumers (Nebula etc.) now."
    echo "  Then: N=${N} BROKER=${BROKER} TOPOLOGY=${TOPOLOGY} BAG_PATH=... ./run.sh --stage robots"
    exit 0
fi

# === Stage: robots (or full run) ===
if [[ "${STAGE}" == "robots" && ! -f "${FLEET_COMPOSE}" ]]; then
    echo "ERROR: ${FLEET_COMPOSE} not found — run './run.sh --stage brokers' first" >&2
    exit 1
fi

# Start robots in batches to avoid DDS multicast burst on host network.
echo "[fleet] starting ${N} robots..."
BATCH=5
for ((i=1; i<=N; i+=BATCH)); do
    batch_services=""
    for ((j=i; j<i+BATCH && j<=N; j++)); do
        batch_services+="robot_${j} "
    done
    NUM_ROBOTS="${N}" MSG_TYPE="${MSG_TYPE}" \
        docker compose "${COMPOSE_ARGS[@]}" up -d --no-deps --remove-orphans ${batch_services}
    [[ $((i + BATCH)) -le N ]] && sleep 3
done

bringup_pad=$(( 15 + N / 2 ))
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
        echo "  Consumer:"
        echo "    docker run --rm --network host ros2-fleet-consumer --broker kafka"
        ;;
    mqtt)
        if [[ "${TOPOLOGY}" == "per-robot" ]]; then
            echo "  Brokers   : localhost:1883 … localhost:$((1882 + N)) (one per robot)"
            echo "  Topics    : ros2/robot_<id>/gnss | /odom | /scan | /points"
            echo ""
            echo "  Quick check (robot 1):"
            echo "    mosquitto_sub -h localhost -p 1883 -t 'ros2/#' -v"
            echo "  Quick check (robot 2):"
            echo "    mosquitto_sub -h localhost -p 1884 -t 'ros2/#' -v"
        else
            echo "  Broker    : localhost:1883"
            echo "  Topics    : ros2/robot_<id>/gnss | /odom | /scan | /points"
            echo ""
            echo "  Quick check:"
            echo "    mosquitto_sub -h localhost -p 1883 -t 'ros2/#' -v"
            echo ""
            echo "  Consumer:"
            echo "    docker run --rm --network host ros2-fleet-consumer --broker mqtt"
        fi
        ;;
esac
echo ""
echo "  To stop: ./run.sh --stop"
