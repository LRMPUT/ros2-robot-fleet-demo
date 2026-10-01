#!/usr/bin/env bash
# Run Nebula geofence latency sweeps for N in {1,5,10,25,50}.
# Uses the repo-local bag by default (override with BAG_PATH).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

DEFAULT_BAG="${SCRIPT_DIR}/bags/rorbots_follower_leader_parcelle_1MONT_ros2"
BAG_PATH="${BAG_PATH:-${DEFAULT_BAG}}"

if [[ ! -f "${BAG_PATH}/metadata.yaml" ]]; then
    echo "ERROR: ${BAG_PATH}/metadata.yaml not found." >&2
    echo "Set BAG_PATH to a converted ROS 2 bag directory." >&2
    exit 2
fi
BAG_PATH="$(cd "${BAG_PATH}" && pwd)"
export BAG_PATH

for i in 1 5 10 25 50; do
    echo "Running with i=$i"

    N="$i" BROKER=mqtt MSG_TYPE=navsatfix TOPOLOGY=per-robot PAYLOAD_FORMAT=json \
        ./run.sh --stage brokers

    (
        cd NEBULA_FLEET/
        N="$i" ./run_nebula.sh
    )

    N="$i" BROKER=mqtt MSG_TYPE=navsatfix TOPOLOGY=per-robot PAYLOAD_FORMAT=json \
        BAG_PATH="${BAG_PATH}" ./run.sh --stage robots

    sleep 400

    (
        cd NEBULA_FLEET/
        docker compose down
    )

    N="$i" BROKER=mqtt TOPOLOGY=per-robot ./run.sh --stop

    sleep 20
done
