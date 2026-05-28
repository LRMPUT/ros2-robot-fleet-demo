#!/usr/bin/env bash
# One-shot fleet latency capture.
#
# Runs the documented 3-stage flow so the consumer is listening before any
# robot publishes, captures for a fixed duration, then tears everything down.
# Produces:
#   <OUTPUT_DIR>/consumer.jsonl               (per-message latency)
#   <OUTPUT_DIR>/publisher/publisher_robot_*.jsonl  (per-message publish times)
#
# Env:
#   BAG_PATH    path to a converted ROS 2 bag dir (default: INRAE bag if present)
#   N           number of robots (default: 10)
#   BROKER      kafka (default) | mqtt
#   DURATION    capture seconds (default: 60)
#   OUTPUT_DIR  artifacts dir (default: latency_artifacts/<timestamp>)
#
# Usage:
#   docker build -t ros2-fleet-consumer consumer/        # once
#   BAG_PATH=bags/..._ros2 N=10 BROKER=kafka DURATION=60 ./tools/run_latency_capture.sh
#   python3 tools/analyze_latency.py latency_artifacts/<run>/
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_DIR}"

N="${N:-10}"
BROKER="${BROKER:-kafka}"
DURATION="${DURATION:-60}"
DEFAULT_BAG="bags/rorbots_follower_leader_parcelle_1MONT_ros2"
BAG_PATH="${BAG_PATH:-${DEFAULT_BAG}}"
OUTPUT_DIR="${OUTPUT_DIR:-latency_artifacts/$(date +%Y%m%d_%H%M%S)}"

if [[ ! -f "${BAG_PATH}/metadata.yaml" ]]; then
    echo "ERROR: ${BAG_PATH}/metadata.yaml not found." >&2
    echo "Set BAG_PATH to a converted ROS 2 bag directory (see README)." >&2
    exit 2
fi

if ! docker image inspect ros2-fleet-consumer >/dev/null 2>&1; then
    echo "ERROR: 'ros2-fleet-consumer' image not found. Build it first:" >&2
    echo "  docker build -t ros2-fleet-consumer consumer/" >&2
    exit 3
fi

ABS_OUT="$(mkdir -p "${OUTPUT_DIR}/publisher" && cd "${OUTPUT_DIR}" && pwd)"
CONSUMER_CID=""

cleanup() {
    echo "[capture] tearing down..."
    if [[ -n "${CONSUMER_CID}" ]]; then
        docker stop "${CONSUMER_CID}" >/dev/null 2>&1 || true
    fi
    N="${N}" BROKER="${BROKER}" "${REPO_DIR}/run.sh" --stop || true
}
trap cleanup EXIT INT TERM

echo "============================================="
echo "  Fleet latency capture"
echo "  Robots   : ${N}"
echo "  Broker   : ${BROKER}"
echo "  Duration : ${DURATION}s"
echo "  Bag      : ${BAG_PATH}"
echo "  Output   : ${ABS_OUT}"
echo "============================================="

# Stage 1: brokers.
N="${N}" BROKER="${BROKER}" "${REPO_DIR}/run.sh" --stage brokers

# Stage 2: consumer (listening before robots publish).
echo "[capture] starting consumer..."
CONSUMER_CID="$(docker run -d --network host \
    -v "${ABS_OUT}:/logs" \
    ros2-fleet-consumer --broker "${BROKER}" \
    --log-file /logs/consumer.jsonl --stats-only)"
sleep 3

# Stage 3: robots, with publisher logging into <OUTPUT_DIR>/publisher.
# LATENCY_LOG_DIR is forwarded to the robot containers by docker-compose via run.sh.
LATENCY_LOG_DIR="${ABS_OUT}/publisher" \
    N="${N}" BROKER="${BROKER}" BAG_PATH="${BAG_PATH}" \
    "${REPO_DIR}/run.sh" --stage robots

echo "[capture] capturing for ${DURATION}s..."
sleep "${DURATION}"

# Stop consumer first so it flushes; teardown handled by trap.
docker stop "${CONSUMER_CID}" >/dev/null 2>&1 || true
CONSUMER_CID=""

CONSUMER_LINES=$(wc -l < "${ABS_OUT}/consumer.jsonl" 2>/dev/null || echo 0)
shopt -s nullglob
pub_files=("${ABS_OUT}/publisher/publisher_robot_"*.jsonl)
shopt -u nullglob
PUB_LINES=0
if (( ${#pub_files[@]} > 0 )); then
    PUB_LINES=$(cat "${pub_files[@]}" | wc -l)
else
    echo "[capture] WARNING: no publisher_robot_*.jsonl files found in ${ABS_OUT}/publisher/" >&2
fi

echo ""
echo "[capture] done."
echo "  consumer.jsonl : ${CONSUMER_LINES} records"
echo "  publisher/*    : ${PUB_LINES} records"
echo ""
echo "  Analyze with:"
echo "    python3 tools/analyze_latency.py ${ABS_OUT}/"
