#!/usr/bin/env bash
set -e


cd benchmarks
source .venv/bin/activate

cd ..

for i in 5 10 25 50; do
    echo "Running with i=$i"

    docker compose \
    -f docker-compose.kafka.yml \
    -f docker-compose.geoflink.yml \
    up -d   

    N="$i" BROKER=kafka BAG_PATH=/home/jeanluc/ros2-robot-fleet-demo/bags/rorbots_follower_leader_parcelle_1MONT_ros2/ ./run.sh

    cd benchmarks
    python run_benchmark.py --robots "$i" --seconds 60 &

    cd ..

    ./run.sh --stop

    sleep 10
done