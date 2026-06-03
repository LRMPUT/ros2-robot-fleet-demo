#!/usr/bin/env bash
# launch_fleet_bench.sh — all-in-one benchmark launcher.
#
# Flow:
#   1. Clean stop + wipe volumes
#   2. Start infrastructure (broker + ksqlDB + API)
#   3. Wait for broker
#   4. Create 50-partition fleet topics
#   5. ksqlDB schema init
#   6. Wait for API + configure geofence (N robots + 3MONT zone)
#   6a. JVM warmup (stack idle — no robots competing for CPU)
#   7. Start collector in background (topic is empty → latest = first alert)
#   8. Start robots  (collector already listening → first alert captured)
#   9. Wait DURATION seconds
#  10. Stop robots
#  11. Print latency report
#  12. Optionally stop the full stack (STOP_AFTER=1)
#
# Usage:
#   N=5 ./launch_fleet_bench.sh
#   N=5 DURATION=120 ./launch_fleet_bench.sh
#   N=5 DURATION=60 STOP_AFTER=1 ./launch_fleet_bench.sh
#
# Results written to: benchmarks/results/run_<N>.txt
set -euo pipefail

export N="${N:-3}"
export BROKER=kafka
export BAG_PATH="/home/jeanluc/ros2-robot-fleet-demo/bags/rorbots_follower_leader_parcelle_1MONT_ros2/"
DURATION="${DURATION:-600}"
WARMUP="${WARMUP:-5}"
TAIL="${TAIL:-5}"

STOP_AFTER="${STOP_AFTER:-0}"
API_URL="http://localhost:8000"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

BENCH_DIR="${SCRIPT_DIR}/benchmarks"
OUT_FILE="${BENCH_DIR}/results/run_${N}.txt"
COLLECTOR_PID=""

# ── cleanup trap ──────────────────────────────────────────────────────────────
cleanup() {
    if [[ -n "${COLLECTOR_PID}" ]]; then
        kill "${COLLECTOR_PID}" 2>/dev/null || true
        wait "${COLLECTOR_PID}" 2>/dev/null || true
    fi
    if [[ "${STOP_AFTER}" == "1" ]]; then
        echo "[bench] stopping stack..."
        ./run.sh --stop 2>/dev/null || true
        docker compose -f docker-compose.kafka.yml -f docker-compose.ksqldb.yml down -v 2>&1 | tail -3
    fi
}
trap cleanup EXIT INT TERM

echo "========================================================"
echo " Fleet Benchmark: N=${N}  DURATION=${DURATION}s"
echo " Output: ${OUT_FILE}"
echo "========================================================"

# ── 1. Clean stop ─────────────────────────────────────────────────────────────
echo "[1/9] Dropping existing cluster states..."
./run.sh --stop 2>/dev/null || true
docker compose -f docker-compose.kafka.yml -f docker-compose.ksqldb.yml down -v 2>&1 | tail -3

# ── 2. Start infrastructure ──────────────────────────────────────────────────
echo "[2/9] Starting infrastructure (broker + ksqlDB + API)..."
docker compose -f docker-compose.kafka.yml -f docker-compose.ksqldb.yml up -d 2>&1 | tail -5

# ── 3. Wait for broker ────────────────────────────────────────────────────────
echo "[3/9] Waiting for Kafka broker..."
until docker exec ros2-robot-fleet-demo-broker-1 kafka-broker-api-versions \
        --bootstrap-server 127.0.0.1:9092 >/dev/null 2>&1; do
    sleep 2
done
echo "    Broker ready."

# ── 4. Pre-create 50-partition fleet topics ──────────────────────────────────
echo "[4/9] Creating 50-partition topics..."
for topic in ros2.fleet.gnss ros2.fleet.odom ros2.fleet.scan ros2.fleet.points; do
    docker exec ros2-robot-fleet-demo-broker-1 kafka-topics \
        --bootstrap-server 127.0.0.1:9092 \
        --create --if-not-exists \
        --topic "$topic" --partitions 50 --replication-factor 1 2>/dev/null
done
echo "    Topics ready."

# ── 5. ksqlDB schema init ─────────────────────────────────────────────────────
echo "[5/9] Waiting for ksqlDB server to heal..."
until curl -s http://localhost:8088/healthcheck >/dev/null 2>&1; do
    sleep 2
done

echo "[5/9] ksqlDB schema init..."
docker restart ros2-robot-fleet-demo-ksqldb-init-1 2>/dev/null || true
sleep 5

# ── 6. Wait for API + configure geofence ─────────────────────────────────────
echo "[6/9] Waiting for API..."
for i in $(seq 1 90); do
    status=$(curl -sf "${API_URL}/health" 2>/dev/null \
        | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null \
        || echo "")
    [ "$status" = "healthy" ] && break
    [ "$i" -eq 90 ] && { echo "ERROR: API not healthy after 90s"; exit 1; }
    sleep 1
done
echo "    API healthy."

echo "    Configuring geofence for N=${N} robots..."
(cd "${BENCH_DIR}" && uv run python setup_ksql.py --robots "${N}" --api-url "${API_URL}")

# Wait for ksqlDB to register all persistent queries.
SETTLE=$(( 15 + N / 2 ))
echo "    Waiting ${SETTLE}s for ksqlDB persistent queries..."
sleep "${SETTLE}"

# JVM warmup: stack is idle, no robots competing for CPU.
# Mirrors the manual gap between launch_fleet.sh finishing and the user
# typing the benchmark command. JIT compilation finishes here.
JVM_WARMUP=$(( 20 + N / 2 ))
echo "    Waiting ${JVM_WARMUP}s for JVM warmup (no robots yet)..."
sleep "${JVM_WARMUP}"

# ── 7. Start collector in background ─────────────────────────────────────────
# The alert topic is EMPTY right now, so "latest" == start of topic.
# Every alert, including the very first GPS message from each robot, will
# be recorded. No alerts missed.
echo "[7/9] Starting collector (topic empty — will capture first alert)..."
mkdir -p "${BENCH_DIR}/results"
(cd "${BENCH_DIR}" && uv run python run_tests.py \
    --robots "${N}" \
    --seconds "$(( DURATION + 60 ))" \
    --out "${OUT_FILE}") &
COLLECTOR_PID=$!
# Give consumer time to join group and take its offset on the empty topic.
sleep 5
echo "    Collector PID=${COLLECTOR_PID} ready."

# ── 8. Start robots ───────────────────────────────────────────────────────────
echo "[8/9] Starting ${N} robots..."
./run.sh

# ── 9. Wait DURATION, stop robots, flush collector ───────────────────────────
echo "[9/9] Collecting for ${DURATION}s..."
sleep "${DURATION}"

echo "    Stopping robots..."
./run.sh --stop 2>/dev/null || true

# Flush: give collector time to receive remaining in-flight alerts.
sleep 5
kill "${COLLECTOR_PID}" 2>/dev/null || true
wait "${COLLECTOR_PID}" 2>/dev/null || true
COLLECTOR_PID=""

# ── Report ────────────────────────────────────────────────────────────────────
echo ""
echo "========================================================"
echo " Latency report"
echo "========================================================"
(cd "${BENCH_DIR}" && uv run python calculate_latency.py "${OUT_FILE}" --warmup "${WARMUP}" --tail "${TAIL}")

echo ""
echo "  Results saved to: ${OUT_FILE}"
[[ "${STOP_AFTER}" == "1" ]] \
    && echo "  Stack stopped (STOP_AFTER=1)." \
    || echo "  Stack still running. Stop with: ./stop_fleet.sh"
