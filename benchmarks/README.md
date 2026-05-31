# ksqlDB geofence latency benchmark

Measures end-to-end latency of the ksqlDB geofence pipeline for N robots
(1..50). Records four timestamps per alert: t0 event, t1 broker ingest,
t2 ksqlDB emit, t3 consumer arrival.

## Run (example: N=5 robots, 60 s)

Terminal 1 — start the fleet + stack:

```
ros2-robot-fleet-demo$ N=5 ./launch_fleet.sh
```

Terminal 2 — run the benchmark:

```
ros2-robot-fleet-demo/benchmarks$ uv run run_benchmark.py --robots 5 --seconds 60
```

Results are written to `results/run_5.txt` and a latency summary is printed.

## Stop

```
ros2-robot-fleet-demo$ ./stop_fleet.sh
```

# Restart (`stop_fleet.sh` then `launch_fleet.sh`) before each new run for a clean dataset.