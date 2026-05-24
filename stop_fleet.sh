#!/usr/bin/env bash
set -euo pipefail

echo "========================================================"
echo " Terminating N-Robot Fleet and Purging Broker States"
echo "========================================================"

# 1. Stop the simulated ROS 2 robots cleanly
echo "[1/2] Powering down simulated robot containers..."
./run.sh --stop || true

# 2. Drop the Kafka/ksqlDB broker containers and clear data volumes
echo "[2/2] Tearing down infrastructure core and wiping volume caches..."
docker compose -f docker-compose.kafka.yml -f docker-compose.ksqldb.yml down -v

echo ""
echo "========================================================"
echo " Success! System is completely clean and idle."
echo "========================================================"
