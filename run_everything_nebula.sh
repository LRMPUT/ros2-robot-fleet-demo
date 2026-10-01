set -e

for i in 1 5 10 25 50; do
    echo "Running with i=$i"

    N="$i" BROKER=mqtt MSG_TYPE=navsatfix TOPOLOGY=per-robot PAYLOAD_FORMAT=json ./run.sh --stage brokers

    cd NEBULA_FLEET/

    N="$i" ./run_nebula.sh

    cd ..

    N="$i" BROKER=mqtt MSG_TYPE=navsatfix TOPOLOGY=per-robot PAYLOAD_FORMAT=json BAG_PATH=/home/jeanluc/ros2-robot-fleet-demo/bags/rorbots_follower_leader_parcelle_1MONT_ros2/ ./run.sh --stage robots

    sleep 400

    cd NEBULA_FLEET/

    docker compose down

    cd ..

    N="$i" BROKER=mqtt TOPOLOGY=per-robot ./run.sh --stop

    sleep 20
done 