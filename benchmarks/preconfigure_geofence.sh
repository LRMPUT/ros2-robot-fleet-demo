#!/usr/bin/env bash
# Pre-configure geofence rules BEFORE robots start publishing.
#
# Run this after launch_fleet.sh returns (stack + topics are up, robots
# are still in the bringup_pad window). The geofence queries will be
# listening before the first GPS message arrives.
#
# Usage:
#   N=5 ./preconfigure_geofence.sh
#   N=10 ./preconfigure_geofence.sh --api-url http://localhost:8000
#
# What it does:
#   1. Waits for the API to be healthy
#   2. Runs setup_ksql.py (registers robots + zone + geofence rules)
#   3. Waits an extra settle period for ksqlDB to persist the queries
#
# After this you can start the benchmark collector immediately:
#   uv run run_benchmark.py --robots $N --seconds 60 --skip-setup
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

N="${N:-5}"
API_URL="${1:-http://localhost:8000}"
# accept --api-url <url> flag too
for arg in "$@"; do
    case "$arg" in
        --api-url) shift; API_URL="$1" ;;
        --api-url=*) API_URL="${arg#*=}" ;;
    esac
done

SETTLE_EXTRA="${SETTLE:-$((15 + N / 2))}"   # same formula as run_benchmark.py

echo "========================================================"
echo "  Pre-configure geofence for N=${N} robots"
echo "  API: ${API_URL}"
echo "  Settle: ${SETTLE_EXTRA}s"
echo "========================================================"

# ── 1. Wait for API ──────────────────────────────────────────────────────────
echo "[1/3] Waiting for API to be healthy..."
for i in $(seq 1 60); do
    status=$(curl -sf "${API_URL}/health" 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('status',''))" 2>/dev/null || echo "")
    if [ "$status" = "healthy" ]; then
        echo "      API healthy after ${i}s"
        break
    fi
    if [ "$i" -eq 60 ]; then
        echo "ERROR: API not healthy after 60s. Is the stack running?"
        echo "  Run: cd ~/eu/ros2-robot-fleet-demo && N=${N} ./launch_fleet.sh"
        exit 1
    fi
    sleep 1
done

# ── 2. Register robots + zone + geofence rules ───────────────────────────────
echo "[2/3] Registering ${N} robots and assigning geofence rules..."
uv run python setup_ksql.py --robots "${N}" --api-url "${API_URL}"

# ── 3. Settle — let ksqlDB persist the new queries before robots publish ─────
echo "[3/3] Waiting ${SETTLE_EXTRA}s for ksqlDB queries to reach steady state..."
sleep "${SETTLE_EXTRA}"

echo ""
echo "========================================================"
echo "  Geofence ready. Robots may now start publishing."
echo "========================================================"
echo ""
echo "  To collect (skip setup — already done):"
echo "    uv run run_benchmark.py --robots ${N} --seconds 60 --skip-setup"
echo ""
echo "  Or start the full benchmark without re-running setup:"
echo "    uv run run_tests.py --robots ${N} --seconds 60"
