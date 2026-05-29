# Fleet Latency Capture — Design

**Date:** 2026-05-28
**Branch:** `feat/latency-capture`
**Repo:** `ros2-robot-fleet-demo`

## Goal

Port the structured latency-capture workflow from
`ros2_kafka_dispatcher/tools/latency/` into the fleet demo, so a multi-robot
fleet run produces machine-readable per-message latency logs (not just the
human-readable stdout `consume.py` already prints) and a single orchestration
script ties the whole capture together end to end.

Deliverables:

1. Per-message **JSONL logs** on both the publisher and consumer side.
2. A **one-shot orchestration script** that starts brokers, consumer, and
   robots in the correct order, runs for a fixed duration, and collects all
   artifacts into one output directory.
3. An **analysis script** that joins the two log sides and reports latency
   percentiles, throughput, and drop rate.

## Key architectural difference from the dispatcher tool

The dispatcher tool (`tools/latency/`) publishes `std_msgs/String` whose JSON
payload embeds both `msg_id` and `t0_ns`. The consumer reads both back, so
publisher and consumer records join trivially by `msg_id`.

The fleet has **no `msg_id`**. `robot_replay.py` replays real sensor messages
(`NavSatFix`, `Odometry`, `LaserScan`, `PointCloud2`) and only sets
`header.stamp = time.time_ns()` at publish time (this is `t0_ns`). `consume.py`
reads `header.stamp` back and computes `latency = t1_ns - t0_ns`.

Therefore the join key between publisher and consumer logs is:

```
(robot_id, suffix, t0_ns)
```

`t0_ns` is a nanosecond wall-clock stamp produced by single-threaded per-stream
timers running at ≤ 50 Hz (≥ 20 ms between publishes on a stream). Collisions
within a `(robot_id, suffix)` stream are therefore impossible, so the triple is
a unique key. No new sequence field is required.

## Components

### 1. `consumer/consume.py` — add `--log-file PATH`

When `--log-file` is set, append one JSONL record per received message:

```json
{"robot_id": 3, "suffix": "gnss", "topic": "ros2.robot_3.gnss",
 "t0_ns": 1748400000123456789, "t1_ns": 1748400000124900000,
 "latency_ns": 1443211, "payload_bytes": 142}
```

- The record is written from both the Kafka poll loop and the MQTT
  `on_message` callback, at the same point latency is currently computed.
- Writes are guarded by the existing module-level `_lock` (Kafka path is
  single-threaded; MQTT `on_message` runs on the paho network thread — the lock
  makes both safe).
- File opened line-buffered, appended (`"a"`), closed on exit.
- When `--log-file` is **not** set, behaviour is unchanged.
- Records are written regardless of `--stats-only` (logging and printing are
  independent concerns).

### 2. `robot_replay.py` — add `--latency-log-dir`

New arg `--latency-log-dir`, default `os.environ.get("LATENCY_LOG_DIR")`.

When set, each robot writes `publisher_robot_<id>.jsonl` into that directory,
one record per published message:

```json
{"robot_id": 3, "suffix": "gnss", "topic": "/robot_3/gnss",
 "t0_ns": 1748400000123456789}
```

- Wired into **both** publish paths: `RobotReplay._tick` (single-stream) and
  the per-stream closure in `MultiTopicRobotReplay` (the path the fleet
  actually uses, `MSG_TYPE=multi`).
- One file handle per `(robot_id)`; in `MultiTopicRobotReplay` the four streams
  of one robot share the robot's single file (records are distinguished by the
  `suffix` field). Writes within a process are serialized by the executor's
  per-callback execution, but a `threading.Lock` guards the shared handle
  because `MultiTopicRobotReplay` uses a `MultiThreadedExecutor`.
- File opened line-buffered, appended, flushed per line, closed in the node's
  shutdown path.
- When unset, behaviour is unchanged (no file opened, zero overhead on the
  hot path beyond a single `is None` check).

In the edge topology each robot is its own container with a distinct
`ROBOT_ID`, so `publisher_robot_<id>.jsonl` filenames never collide on the host
bind mount.

### 3. `gen_fleet.sh` — inject log mount when `LATENCY_LOG_DIR` is set

When `LATENCY_LOG_DIR` is present in the environment, add to **each** generated
robot service (both `shared` and `per-robot` topology branches):

```yaml
    environment:
      ...
      LATENCY_LOG_DIR: "/latency"
    volumes:
      ...
      - ${LATENCY_LOG_DIR}:/latency
```

`/latency` is the in-container path; the host path is the orchestrator's output
directory. `robot_replay.py` reads `LATENCY_LOG_DIR` from its environment
(already passed straight through by `edge_entrypoint.sh`, which `exec`s
`python3 /app/robot_replay.py` with the container environment intact), so no
change to `edge_entrypoint.sh` is needed.

When `LATENCY_LOG_DIR` is unset, the generated compose file is byte-for-byte
identical to today's output (backward compatible).

### 4. `tools/run_latency_capture.sh` — orchestration (new)

Drives the capture in three stages so the consumer is listening *before* any
robot publishes (otherwise early messages are missed — the same reason the
README documents the manual 3-stage flow):

1. `run.sh --stage brokers`
2. Start the consumer in Docker, mounting the output dir and writing
   `consumer.jsonl`:
   `docker run -d --network host -v "$OUTPUT_DIR":/logs ros2-fleet-consumer
   --broker "$BROKER" --log-file /logs/consumer.jsonl`
3. `LATENCY_LOG_DIR="$OUTPUT_DIR/publisher" run.sh --stage robots`
4. `sleep "$DURATION"`
5. Stop the consumer container, then `run.sh --stop`.
6. Print a summary: output dir, line counts of each log, next-step analyzer
   command.

Configuration (env vars, matching the repo's existing `run.sh` convention):

| Var            | Default                                                   |
|----------------|-----------------------------------------------------------|
| `BAG_PATH`     | `bags/rorbots_follower_leader_parcelle_1MONT_ros2` if present, else required |
| `N`            | `10`                                                      |
| `BROKER`       | `kafka`                                                   |
| `DURATION`     | `60` (seconds)                                            |
| `OUTPUT_DIR`   | `latency_artifacts/<timestamp>`                          |

- A `trap` on `EXIT`/`INT`/`TERM` stops the consumer container and tears the
  fleet down, so Ctrl+C never leaves containers running.
- Requires the `ros2-fleet-consumer` image (documented prerequisite; the
  script checks for it and prints the `docker build` command if missing).
- The publisher output subdirectory (`$OUTPUT_DIR/publisher`) is created before
  stage 3 so the bind mount target exists.

### 5. `tools/analyze_latency.py` — analysis (new, net-new vs dispatcher tool)

Reads `consumer.jsonl` and the `publisher_robot_*.jsonl` files from an output
directory and reports:

- **Per stream (suffix)** and **per robot**: count, p50 / p95 / p99 / max
  latency in milliseconds, mean throughput (msg/s over the capture window).
- **Drop rate**: `(published − received) / published` per `(robot_id, suffix)`,
  computed by joining on `(robot_id, suffix, t0_ns)`. Published count comes from
  the publisher JSONL line counts; received from matched consumer records.
- An **overall summary** line.

Implementation notes:

- Pure standard library (`json`, `statistics`, `argparse`, `glob`) — no pandas
  dependency, so it runs anywhere Python 3 is available.
- Latency is taken from the consumer record's `latency_ns` field (already
  computed); the publisher join is used only for drop accounting, so a missing
  publisher directory degrades gracefully to "latency only, drop rate N/A".
- Usage: `python3 tools/analyze_latency.py latency_artifacts/<run>/`

### 6. README — "Latency capture" section

Document the one-command flow:

```bash
# 10-robot Kafka fleet, 60 s capture
docker build -t ros2-fleet-consumer consumer/        # once
BAG_PATH=bags/..._ros2 N=10 BROKER=kafka DURATION=60 \
    ./tools/run_latency_capture.sh
python3 tools/analyze_latency.py latency_artifacts/<run>/
```

Describe the output artifacts and the `(robot_id, suffix, t0_ns)` join key.

## Data flow

```
robot_replay.py  --(header.stamp = t0_ns)-->  per-robot sink --> broker --> consume.py
      |                                                                         |
      v                                                                         v
publisher_robot_<id>.jsonl                                               consumer.jsonl
      \________________________ join on (robot_id, suffix, t0_ns) _______________________/
                                          |
                                          v
                                 analyze_latency.py
                          (p50/p95/p99, throughput, drop rate)
```

## Testing

- **`consume.py` logging**: unit test that a crafted message produces one
  well-formed JSONL line with the expected keys and a positive `latency_ns`.
  Reuse the existing `tests/` + `conftest.py` setup.
- **`robot_replay.py` logging**: unit test that enabling `--latency-log-dir`
  causes `_tick` to append a record with `t0_ns` matching the published
  `header.stamp`; disabled → no file created.
- **`gen_fleet.sh`**: assert generated compose contains the volume + env when
  `LATENCY_LOG_DIR` is set, and is unchanged when unset.
- **`analyze_latency.py`**: unit test on small synthetic publisher/consumer
  JSONL fixtures with a known dropped message → expected percentiles and drop
  rate.
- **End-to-end** (manual, documented): a short `DURATION=20 N=3` Kafka run that
  produces non-empty logs and a sane analyzer report. Not run in CI (needs
  Docker + broker).

## Out of scope

- Changing the latency definition (stays `t1 - header.stamp`).
- Wall-clock skew correction between containers (all run on one host; clock is
  shared).
- Plotting (the analyzer prints tables; trajectory plotting already exists
  separately under `tools/plot_trajectories.py`).
