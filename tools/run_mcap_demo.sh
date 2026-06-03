#!/usr/bin/env bash
# Run the MCAP bag demo: MQTT broker → consumer echo → live matplotlib viz.
#
# Opens three parallel processes:
#   1. Eclipse Mosquitto (Docker, port 1883)
#   2. consumer/consume.py  — echoes every GNSS message to stdout
#   3. tools/live_gnss_viz.py — live matplotlib trajectory window
#   4. tools/publish_mcap_to_mqtt.py — replays the MCAP bag (foreground)
#
# Usage:
#   ./tools/run_mcap_demo.sh
#   BAG=bags/rosbag2_2026_04_10-11_01_18 SPEED=2.0 ./tools/run_mcap_demo.sh
#
# Press Ctrl+C to stop everything.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="${SCRIPT_DIR}/.."
cd "${ROOT}"

BAG="${BAG:-bags/rosbag2_2026_04_10-11_01_18}"
SPEED="${SPEED:-1.0}"
LOOP="${LOOP:-}"   # set to "--loop" to replay indefinitely

# ── 1. Start MQTT broker ──────────────────────────────────────────────────────
echo "[demo] Starting MQTT broker..."
docker compose -f docker-compose.mqtt.yml -p mcap-demo up -d

for i in $(seq 1 30); do
    status=$(docker inspect --format '{{.State.Health.Status}}' mcap-demo-broker-1 2>/dev/null || echo "unknown")
    [ "$status" = "healthy" ] && break
    echo "[demo] Waiting for broker (${i}s)..."
    sleep 1
done
echo "[demo] Broker healthy."

# ── cleanup trap ─────────────────────────────────────────────────────────────
PIDS=()
cleanup() {
    echo ""
    echo "[demo] Stopping..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    docker compose -f docker-compose.mqtt.yml -p mcap-demo down 2>&1 | tail -2
    echo "[demo] Done."
}
trap cleanup EXIT INT TERM

# ── 2. Consumer echo ─────────────────────────────────────────────────────────
echo "[demo] Starting consumer echo..."
python3 consumer/consume.py --broker mqtt --format json &
PIDS+=($!)
sleep 0.5

# ── 3. Live matplotlib viz ───────────────────────────────────────────────────
echo "[demo] Opening live viz window..."
python3 tools/live_gnss_viz.py &
PIDS+=($!)
sleep 1

# ── 4. MCAP publisher (foreground — Ctrl+C stops everything) ─────────────────
echo "[demo] Replaying bag at ${SPEED}x speed... (Ctrl+C to stop)"
echo ""
python3 tools/publish_mcap_to_mqtt.py \
    --bag "${BAG}" \
    --speed "${SPEED}" \
    ${LOOP}

wait
