#!/usr/bin/env bash
set -euo pipefail

# Define configuration constants
export N="${N:-3}"
export BROKER=kafka
export BAG_PATH="/home/jeanluc/ros2-robot-fleet-demo/bags/rorbots_follower_leader_parcelle_1MONT_ros2/"

echo "========================================================"
echo " Starting Automated N-Robot Fleet"
echo "========================================================"

# 1. Clean stop and purge old volume metadata cache
echo "[1/5] Dropping existing cluster states and clearing volume caches..."
./run.sh --stop || true
docker compose -f docker-compose.kafka.yml -f docker-compose.ksqldb.yml down -v

# 2. Launch the core backend infrastructure engines
echo "[2/5] Initializing database core layout (Broker, ksqlDB)..."
docker compose -f docker-compose.kafka.yml -f docker-compose.ksqldb.yml up -d

# 3. Wait dynamically for the Kafka broker port to accept connections
echo "[3/5] Waiting for Kafka broker to accept socket connections..."
until docker exec ros2-robot-fleet-demo-broker-1 kafka-broker-api-versions --bootstrap-server 127.0.0.1:9092 >/dev/null 2>&1; do
    echo "    -> Broker initializing internal metadata... retrying in 2 seconds..."
    sleep 2
done

# 4. Explicitly pre-create ALL 4 fleet streams with 50 partitions 
echo "[4/5] Pre-carving 50-lane partition layouts on the broker..."
for topic in ros2.fleet.gnss ros2.fleet.odom ros2.fleet.scan ros2.fleet.points; do
    docker exec ros2-robot-fleet-demo-broker-1 kafka-topics \
        --bootstrap-server 127.0.0.1:9092 \
        --create \
        --if-not-exists \
        --topic "$topic" \
        --partitions 50 \
        --replication-factor 1
    echo "    -> Successfully carved topic: $topic (50 channels)"
done

# 5. Restart the init service to execute SQL statements cleanly against the ready infrastructure
echo "[5/5] Waiting for ksqlDB server to heal..."
until curl -s http://localhost:8088/healthcheck >/dev/null 2>&1; do
    sleep 2
done

echo "    ksqlDB server ready. Running ksqlDB schema definitions..."
docker restart ros2-robot-fleet-demo-ksqldb-init-1

echo ""
echo "========================================================"
echo " Success! All 50-lane highways are ready and validated."
echo "========================================================"
echo ""

# Launch the N-robot simulations onto the prepared N-lane layout
./run.sh
