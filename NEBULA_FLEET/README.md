# Running a NebulaStream fleet

1. In `ros2-robot-fleet-demo` directory
```
N=? \
BROKER=mqtt \
TOPOLOGY=per-robot \
PAYLOAD_FORMAT=json \
./run.sh --stage brokers
```

2. In `NEBULA_FLEET` directory
```
N=? ./run_nebula.sh
```
Wait until completion of the script. Connect to NebulaStream on `http://localhost:9000/` (use `localhost` and `8081` as REST port) and check if all the workers are working properly and the query has `RUNNING` status

3. In `ros2-robot-fleet-demo` directory
```
N=? \
BROKER=mqtt \
TOPOLOGY=per-robot \
PAYLOAD_FORMAT=json \
BAG_PATH=/path/to/ros2/directory/ \
./run.sh --stage robots
```

4. After running the file disconnect the NebulaStream Fleet (in `NEBULA_FLEET` directory)
```
docker compose down
```

5. Stop the robotic fleet (in `ros2-robot-fleet-demo`)
```
N=6 \
BROKER=mqtt \
TOPOLOGY=per-robot \
./run.sh --stop
```

6. Timestamps are stored in `NEBULA_FLEET/logs/timestamps.txt`. This file stores only messages that went throught the query
```
robot_id	message_ts_sec	message_ts_nanosec	processing_ts	arrival_ts
robot_8	1779990028	994837515	1779990028996558715	1779990029011333353
```