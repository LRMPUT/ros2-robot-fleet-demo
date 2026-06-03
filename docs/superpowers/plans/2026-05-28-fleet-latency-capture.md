# Fleet Latency Capture Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add per-message JSONL latency logging to the fleet's publisher and consumer, plus a one-shot orchestration script and an analysis script, so a multi-robot run produces machine-readable latency/throughput/drop-rate data.

**Architecture:** Logging logic is extracted into small, ROS-free helper units (`PublisherLatencyLogger` in `robot_replay.py`, `_log_latency` in `consume.py`) so it is unit-testable with the repo's existing MagicMock-stub pattern. `gen_fleet.sh` conditionally bind-mounts a host log dir into each robot container when `LATENCY_LOG_DIR` is set. `tools/run_latency_capture.sh` runs the documented 3-stage flow (brokers → consumer → robots) to a fixed duration. `tools/analyze_latency.py` joins the two log sides on `(robot_id, suffix, t0_ns)` sets (robust to MQTT QoS-1 duplicates) and reports percentiles and drop rate.

**Tech Stack:** Python 3 stdlib (`json`, `threading`, `statistics`, `glob`, `argparse`), Bash, Docker Compose, pytest with MagicMock stubs.

---

## File Structure

- **Modify** `consumer/consume.py` — add module-level `_log_fh`/`_log_lock` and `_log_latency(...)` helper; add `--log-file` arg; call helper from both broker paths.
- **Modify** `robot_replay.py` — add `PublisherLatencyLogger` class; add `--latency-log-dir` arg; store `suffix`/`topic` on streams; call logger after each publish; close on shutdown.
- **Modify** `gen_fleet.sh` — when `LATENCY_LOG_DIR` is set, inject `LATENCY_LOG_DIR` env var and `${LATENCY_LOG_DIR}:/latency` volume into each robot service (both topology branches).
- **Create** `tools/run_latency_capture.sh` — 3-stage orchestration + teardown trap.
- **Create** `tools/analyze_latency.py` — set-join analyzer: p50/p95/p99/max latency, throughput, drop rate.
- **Create** `tests/test_consume_logging.py` — unit test for `_log_latency`.
- **Create** `tests/test_publisher_logging.py` — unit test for `PublisherLatencyLogger`.
- **Create** `tests/test_gen_fleet_latency.py` — subprocess test for compose injection.
- **Create** `tests/test_analyze_latency.py` — unit test for analyzer math.
- **Modify** `README.md` — add "Latency capture" section.

All work happens on branch `feat/latency-capture` (already created).

---

## Task 1: Consumer JSONL logging (`consume.py`)

**Files:**
- Modify: `consumer/consume.py`
- Test: `tests/test_consume_logging.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_consume_logging.py`:

```python
"""Tests for JSONL latency logging in consume.py."""
import io
import json
import sys
from unittest.mock import MagicMock

# Stub broker libraries so consume.py imports without them installed.
sys.modules.setdefault("confluent_kafka", MagicMock())
sys.modules.setdefault("paho", MagicMock())
sys.modules.setdefault("paho.mqtt", MagicMock())
sys.modules.setdefault("paho.mqtt.client", MagicMock())

import consumer.consume as consume


def _reset_log():
    consume._log_fh = None


def test_log_latency_noop_when_disabled():
    _reset_log()
    # Must not raise when logging is off.
    consume._log_latency(1, "gnss", "ros2.robot_1.gnss", 100, 200, 100, 42)


def test_log_latency_writes_one_jsonl_record():
    buf = io.StringIO()
    consume._log_fh = buf
    try:
        consume._log_latency(3, "gnss", "ros2.robot_3.gnss",
                             1000, 1500, 500, 142)
    finally:
        consume._log_fh = None

    lines = buf.getvalue().strip().splitlines()
    assert len(lines) == 1
    rec = json.loads(lines[0])
    assert rec == {
        "robot_id": 3,
        "suffix": "gnss",
        "topic": "ros2.robot_3.gnss",
        "t0_ns": 1000,
        "t1_ns": 1500,
        "latency_ns": 500,
        "payload_bytes": 142,
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/maciej/Github/ros2-robot-fleet-demo && python3 -m pytest tests/test_consume_logging.py -v`
Expected: FAIL with `AttributeError: module 'consumer.consume' has no attribute '_log_fh'` (or `_log_latency`).

- [ ] **Step 3: Add the logging helper to `consume.py`**

In `consumer/consume.py`, after the existing module-level state block (after the line `_warned_no_data: bool = False  # ...`), add:

```python
# JSONL latency log. _log_fh is None unless --log-file is given.
_log_fh = None
_log_lock = threading.Lock()


def _log_latency(robot_id: int, suffix: str, topic: str,
                 t0_ns: int, t1_ns: int, latency_ns: int,
                 payload_bytes: int) -> None:
    """Append one JSONL latency record. No-op when logging is disabled."""
    if _log_fh is None:
        return
    rec = _json.dumps({
        "robot_id": robot_id,
        "suffix": suffix,
        "topic": topic,
        "t0_ns": t0_ns,
        "t1_ns": t1_ns,
        "latency_ns": latency_ns,
        "payload_bytes": payload_bytes,
    }, separators=(",", ":"))
    with _log_lock:
        _log_fh.write(rec + "\n")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/maciej/Github/ros2-robot-fleet-demo && python3 -m pytest tests/test_consume_logging.py -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Wire `_log_latency` into both broker paths and add the CLI flag**

In `consume_kafka`, immediately after the existing line
`_record(msg.topic(), len(msg.value()), robot_id=robot_id)` add:

```python
        _log_latency(robot_id, suffix, msg.topic(),
                     t0_ns, t1_ns, t1_ns - t0_ns, len(msg.value()))
```

In `consume_mqtt`'s `on_message`, immediately after the existing line
`_record(msg.topic, len(msg.payload), robot_id=robot_id)` add:

```python
        _log_latency(robot_id, msg.topic, msg.topic,
                     t0_ns, t1_ns, t1_ns - t0_ns, len(msg.payload))
```

Note: the MQTT topic string (e.g. `ros2/robot_3/gnss`) is passed as both
`suffix`-bearing topic and topic; the `suffix` column for MQTT comes from
`parsed`. Replace the MQTT call with the precise form using the already-unpacked
`suffix`:

```python
        _log_latency(robot_id, suffix, msg.topic,
                     t0_ns, t1_ns, t1_ns - t0_ns, len(msg.payload))
```

(Use this second form — `suffix` is already unpacked from `parsed` in
`on_message`.)

In `main()`, add the CLI argument after the existing `--silence-threshold`
argument:

```python
    parser.add_argument("--log-file", default=None,
                        help="Append per-message latency JSONL to this path")
```

And after `args = parser.parse_args()`, open the file:

```python
    global _log_fh
    if args.log_file:
        _log_fh = open(args.log_file, "a", buffering=1)
```

- [ ] **Step 6: Run the full consumer test suite to verify no regressions**

Run: `cd /home/maciej/Github/ros2-robot-fleet-demo && python3 -m pytest tests/test_consume_logging.py tests/test_consume_health.py -v`
Expected: PASS (all tests).

- [ ] **Step 7: Commit**

```bash
cd /home/maciej/Github/ros2-robot-fleet-demo
git add consumer/consume.py tests/test_consume_logging.py
git commit -m "feat(consumer): add --log-file JSONL latency logging"
```

---

## Task 2: Publisher JSONL logging (`robot_replay.py`)

**Files:**
- Modify: `robot_replay.py`
- Test: `tests/test_publisher_logging.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_publisher_logging.py`:

```python
"""Tests for PublisherLatencyLogger in robot_replay.py."""
import json
import sys
from unittest.mock import MagicMock

# Stub ROS imports so robot_replay imports without a ROS install.
for mod in [
    "rosbag2_py", "rclpy", "rclpy.node", "rclpy.serialization",
    "rclpy.executors", "rosidl_runtime_py", "rosidl_runtime_py.utilities",
    "nav_msgs", "nav_msgs.msg", "sensor_msgs", "sensor_msgs.msg",
]:
    sys.modules.setdefault(mod, MagicMock())

from robot_replay import PublisherLatencyLogger  # noqa: E402


def test_logger_writes_one_record_per_call(tmp_path):
    logger = PublisherLatencyLogger(str(tmp_path), robot_id=7)
    logger.record("gnss", "/robot_7/gnss", 123)
    logger.record("odom", "/robot_7/odom", 456)
    logger.close()

    log_path = tmp_path / "publisher_robot_7.jsonl"
    assert log_path.exists()
    lines = log_path.read_text().strip().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0]) == {
        "robot_id": 7, "suffix": "gnss", "topic": "/robot_7/gnss", "t0_ns": 123,
    }
    assert json.loads(lines[1])["suffix"] == "odom"


def test_logger_filename_uses_robot_id(tmp_path):
    logger = PublisherLatencyLogger(str(tmp_path), robot_id=2)
    logger.close()
    assert (tmp_path / "publisher_robot_2.jsonl").exists()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/maciej/Github/ros2-robot-fleet-demo && python3 -m pytest tests/test_publisher_logging.py -v`
Expected: FAIL with `ImportError: cannot import name 'PublisherLatencyLogger'`.

- [ ] **Step 3: Add `PublisherLatencyLogger` and stdlib imports**

In `robot_replay.py`, add to the top-level imports (after `import time`):

```python
import json
import threading
```

Then add the class after the imports block (before `LAT_OFFSET_DEG_PER_ID`):

```python
class PublisherLatencyLogger:
    """Append-only JSONL logger of published-message timestamps.

    One file per robot (`publisher_robot_<id>.jsonl`). Thread-safe because
    MultiTopicRobotReplay drives multiple stream timers on a
    MultiThreadedExecutor that share one robot's logger.
    """

    def __init__(self, log_dir: str, robot_id: int) -> None:
        import os
        os.makedirs(log_dir, exist_ok=True)
        path = os.path.join(log_dir, f"publisher_robot_{robot_id}.jsonl")
        self._fh = open(path, "a", buffering=1)
        self._lock = threading.Lock()
        self._robot_id = robot_id

    def record(self, suffix: str, topic: str, t0_ns: int) -> None:
        line = json.dumps({
            "robot_id": self._robot_id,
            "suffix": suffix,
            "topic": topic,
            "t0_ns": t0_ns,
        }, separators=(",", ":"))
        with self._lock:
            self._fh.write(line + "\n")

    def close(self) -> None:
        with self._lock:
            self._fh.close()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/maciej/Github/ros2-robot-fleet-demo && python3 -m pytest tests/test_publisher_logging.py -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Wire the logger into `RobotReplay`**

In `RobotReplay.__init__`, change the signature to accept a logger and store
the suffix/topic. Replace the existing `__init__` body's relevant lines:

Change the signature line:
```python
    def __init__(self, robot_id: int, bag_path: str, rate_hz: float,
                 msg_type: str = "navsatfix", latency_logger=None) -> None:
```

After the existing `type_str, type_class, suffix, _native_rate = TYPE_CONFIG[msg_type]`
line, add storage of suffix/topic and the logger:
```python
        self._suffix = suffix
        self._topic = f"/robot_{robot_id}/{suffix}"
        self._latency_logger = latency_logger
```

Replace the existing `self._tick` method body to log after publish:
```python
    def _tick(self) -> None:
        msg = next(self._looper)
        shift_message(msg, self._robot_id, self._msg_type)
        t0_ns = time.time_ns()
        restamp_ns(msg, t0_ns)
        self._pub.publish(msg)
        if self._latency_logger is not None:
            self._latency_logger.record(self._suffix, self._topic, t0_ns)
```

- [ ] **Step 6: Wire the logger into `MultiTopicRobotReplay`**

In `MultiTopicRobotReplay.__init__`, change the signature:
```python
    def __init__(self, robot_id: int, bag_path: str, types=MULTI_TYPES,
                 latency_logger=None) -> None:
```

Store the logger right after `self._robot_id = robot_id`:
```python
        self._latency_logger = latency_logger
```

Inside the `make_tick` closure, capture the suffix and topic and log after
publish. Replace the existing `make_tick` definition with:
```python
            topic = f"/robot_{robot_id}/{suffix}"
            def make_tick(_looper=looper, _pub=pub, _short=short,
                          _suffix=suffix, _topic=topic):
                def tick():
                    try:
                        msg = next(_looper)
                    except RuntimeError:
                        return
                    shift_message(msg, self._robot_id, _short)
                    t0_ns = time.time_ns()
                    restamp_ns(msg, t0_ns)
                    _pub.publish(msg)
                    if self._latency_logger is not None:
                        self._latency_logger.record(_suffix, _topic, t0_ns)
                return tick
```

- [ ] **Step 7: Add the CLI arg and construct loggers in `main()`**

In `main()`'s argument parser, after the `--msg-type` argument add:
```python
    parser.add_argument(
        "--latency-log-dir",
        default=os.environ.get("LATENCY_LOG_DIR"),
        help="If set, write publisher_robot_<id>.jsonl per robot here.",
    )
```

Replace the node-construction block in `main()` so each node gets a logger.
The fleet-mode loop becomes:
```python
        if args.num_robots > 0:
            from rclpy.executors import MultiThreadedExecutor
            nthreads = min(max(args.num_robots, 4) * (4 if multi else 1), 32)
            executor = MultiThreadedExecutor(num_threads=nthreads)
            for robot_id in range(1, args.num_robots + 1):
                logger = (PublisherLatencyLogger(args.latency_log_dir, robot_id)
                          if args.latency_log_dir else None)
                if multi:
                    node = MultiTopicRobotReplay(robot_id, args.bag_path,
                                                 latency_logger=logger)
                else:
                    node = RobotReplay(robot_id, args.bag_path, args.rate_hz,
                                       args.msg_type, latency_logger=logger)
                nodes.append(node)
                executor.add_node(node)
            print(f"[robot_replay] Fleet mode: {args.num_robots} robots "
                  f"type={args.msg_type} threads={nthreads}", flush=True)
            executor.spin()
```

The single-robot branch becomes:
```python
        else:
            robot_id = args.robot_id if args.robot_id >= 0 else derive_robot_id_from_hostname()
            logger = (PublisherLatencyLogger(args.latency_log_dir, robot_id)
                      if args.latency_log_dir else None)
            if multi:
                node = MultiTopicRobotReplay(robot_id, args.bag_path,
                                             latency_logger=logger)
            else:
                node = RobotReplay(robot_id, args.bag_path, args.rate_hz,
                                   args.msg_type, latency_logger=logger)
            nodes.append(node)
            if multi:
                from rclpy.executors import MultiThreadedExecutor
                ex = MultiThreadedExecutor(num_threads=4)
                ex.add_node(node)
                ex.spin()
            else:
                rclpy.spin(node)
```

- [ ] **Step 8: Run the publisher tests + existing bag-looper tests**

Run: `cd /home/maciej/Github/ros2-robot-fleet-demo && python3 -m pytest tests/test_publisher_logging.py tests/test_bag_looper.py -v`
Expected: PASS (all tests).

- [ ] **Step 9: Commit**

```bash
cd /home/maciej/Github/ros2-robot-fleet-demo
git add robot_replay.py tests/test_publisher_logging.py
git commit -m "feat(robot_replay): add --latency-log-dir publisher JSONL logging"
```

---

## Task 3: Compose log-mount injection (`gen_fleet.sh`)

**Files:**
- Modify: `gen_fleet.sh`
- Test: `tests/test_gen_fleet_latency.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_gen_fleet_latency.py`:

```python
"""gen_fleet.sh injects latency log mount only when LATENCY_LOG_DIR is set."""
import os
import pathlib
import subprocess

REPO = pathlib.Path(__file__).resolve().parents[1]
GEN = REPO / "gen_fleet.sh"


def _run(out_file, env_extra):
    env = dict(os.environ, BROKER="kafka", MSG_TYPE="multi", **env_extra)
    subprocess.run(["bash", str(GEN), "2", str(out_file)],
                   check=True, env=env, capture_output=True, text=True)
    return out_file.read_text()


def test_no_injection_when_unset(tmp_path):
    out = tmp_path / "fleet.yml"
    text = _run(out, {})
    assert "LATENCY_LOG_DIR" not in text
    assert ":/latency" not in text


def test_injection_when_set(tmp_path):
    out = tmp_path / "fleet.yml"
    text = _run(out, {"LATENCY_LOG_DIR": "/host/logs"})
    # Env var present for both robots.
    assert text.count('LATENCY_LOG_DIR: "/latency"') == 2
    # Volume mount references the compose-substituted host path var.
    assert text.count("${LATENCY_LOG_DIR}:/latency") == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/maciej/Github/ros2-robot-fleet-demo && python3 -m pytest tests/test_gen_fleet_latency.py -v`
Expected: FAIL on `test_injection_when_set` (count == 0, not 2).

- [ ] **Step 3: Build the injection strings and add them to both branches**

In `gen_fleet.sh`, after the line `IMAGE="ghcr.io/lrmput/ros2-kafka-dispatcher:latest"`,
add:

```bash
# Optional per-robot latency log mount. Empty unless LATENCY_LOG_DIR is set in
# the environment (the run_latency_capture.sh orchestrator sets it). The
# ${LATENCY_LOG_DIR} reference stays literal so Docker Compose substitutes the
# host path at `up` time.
LATENCY_ENV=""
LATENCY_VOL=""
if [[ -n "${LATENCY_LOG_DIR:-}" ]]; then
    LATENCY_ENV=$'\n      LATENCY_LOG_DIR: "/latency"'
    LATENCY_VOL=$'\n      - ${LATENCY_LOG_DIR}:/latency'
fi
```

In the **per-robot** heredoc, change the `PAYLOAD_FORMAT` env line and the
`robot_replay.py` volume line so the injection appends after them:

Find:
```
      PAYLOAD_FORMAT: "${PAYLOAD_FORMAT}"
    volumes:
```
Replace with:
```
      PAYLOAD_FORMAT: "${PAYLOAD_FORMAT}"${LATENCY_ENV}
    volumes:
```

Find (per-robot branch):
```
      - ${SCRIPT_DIR}/robot_replay.py:/app/robot_replay.py:ro
    entrypoint: ["/usr/local/bin/edge_entrypoint.sh"]
    depends_on:
      - broker_${i}
```
Replace with:
```
      - ${SCRIPT_DIR}/robot_replay.py:/app/robot_replay.py:ro${LATENCY_VOL}
    entrypoint: ["/usr/local/bin/edge_entrypoint.sh"]
    depends_on:
      - broker_${i}
```

In the **shared** heredoc, find:
```
      PAYLOAD_FORMAT: "${PAYLOAD_FORMAT}"
    volumes:
      - "\${BAG_PATH:?BAG_PATH is required}:/data/bag:ro"
      - ./edge_entrypoint.sh:/usr/local/bin/edge_entrypoint.sh:ro
      - ./robot_replay.py:/app/robot_replay.py:ro
    entrypoint: ["/usr/local/bin/edge_entrypoint.sh"]
    depends_on:
      - broker
```
Replace with:
```
      PAYLOAD_FORMAT: "${PAYLOAD_FORMAT}"${LATENCY_ENV}
    volumes:
      - "\${BAG_PATH:?BAG_PATH is required}:/data/bag:ro"
      - ./edge_entrypoint.sh:/usr/local/bin/edge_entrypoint.sh:ro
      - ./robot_replay.py:/app/robot_replay.py:ro${LATENCY_VOL}
    entrypoint: ["/usr/local/bin/edge_entrypoint.sh"]
    depends_on:
      - broker
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/maciej/Github/ros2-robot-fleet-demo && python3 -m pytest tests/test_gen_fleet_latency.py -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Sanity-check generated YAML is still valid**

Run:
```bash
cd /home/maciej/Github/ros2-robot-fleet-demo
LATENCY_LOG_DIR=/tmp/x BROKER=kafka MSG_TYPE=multi bash gen_fleet.sh 1 /tmp/_fleet_check.yml
python3 -c "import yaml,sys; yaml.safe_load(open('/tmp/_fleet_check.yml')); print('valid yaml')"
```
Expected: prints `valid yaml` (no exception). Then `rm -f /tmp/_fleet_check.yml`.

- [ ] **Step 6: Commit**

```bash
cd /home/maciej/Github/ros2-robot-fleet-demo
git add gen_fleet.sh tests/test_gen_fleet_latency.py
git commit -m "feat(gen_fleet): bind-mount latency log dir when LATENCY_LOG_DIR set"
```

---

## Task 4: Latency analyzer (`tools/analyze_latency.py`)

**Files:**
- Create: `tools/analyze_latency.py`
- Test: `tests/test_analyze_latency.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_analyze_latency.py`:

```python
"""Tests for the latency analyzer's join + stats math."""
import json
import pathlib
import sys

REPO = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "tools"))

import analyze_latency as al  # noqa: E402


def _write_jsonl(path, records):
    with open(path, "w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


def test_analyze_computes_drop_rate_and_percentiles(tmp_path):
    pub_dir = tmp_path / "publisher"
    pub_dir.mkdir()
    # robot 1 gnss published 3 messages (t0 = 100, 200, 300)
    _write_jsonl(pub_dir / "publisher_robot_1.jsonl", [
        {"robot_id": 1, "suffix": "gnss", "topic": "/robot_1/gnss", "t0_ns": 100},
        {"robot_id": 1, "suffix": "gnss", "topic": "/robot_1/gnss", "t0_ns": 200},
        {"robot_id": 1, "suffix": "gnss", "topic": "/robot_1/gnss", "t0_ns": 300},
    ])
    # consumer received 2 of them (t0 = 100, 300); 200 was dropped.
    # A duplicate delivery of 100 must NOT inflate the matched count.
    _write_jsonl(tmp_path / "consumer.jsonl", [
        {"robot_id": 1, "suffix": "gnss", "topic": "ros2.robot_1.gnss",
         "t0_ns": 100, "t1_ns": 1100, "latency_ns": 1000, "payload_bytes": 10},
        {"robot_id": 1, "suffix": "gnss", "topic": "ros2.robot_1.gnss",
         "t0_ns": 100, "t1_ns": 1200, "latency_ns": 1100, "payload_bytes": 10},
        {"robot_id": 1, "suffix": "gnss", "topic": "ros2.robot_1.gnss",
         "t0_ns": 300, "t1_ns": 3000, "latency_ns": 2700, "payload_bytes": 10},
    ])

    report = al.analyze(str(tmp_path))
    gnss = report["by_suffix"]["gnss"]
    assert gnss["published"] == 3
    assert gnss["matched"] == 2          # dedup by t0: {100, 300}
    assert gnss["drop_rate"] == 1 / 3    # 1 of 3 dropped
    # Latency percentiles use ALL consumer records (ns → ms).
    assert gnss["count"] == 3
    assert gnss["p50_ms"] == 1.1         # median of [1.0, 1.1, 2.7] ms


def test_analyze_without_publisher_dir_degrades(tmp_path):
    _write_jsonl(tmp_path / "consumer.jsonl", [
        {"robot_id": 2, "suffix": "odom", "topic": "ros2.robot_2.odom",
         "t0_ns": 5, "t1_ns": 1005, "latency_ns": 1000, "payload_bytes": 8},
    ])
    report = al.analyze(str(tmp_path))
    odom = report["by_suffix"]["odom"]
    assert odom["count"] == 1
    assert odom["published"] is None      # no publisher data
    assert odom["drop_rate"] is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/maciej/Github/ros2-robot-fleet-demo && python3 -m pytest tests/test_analyze_latency.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'analyze_latency'`.

- [ ] **Step 3: Write `tools/analyze_latency.py`**

Create `tools/analyze_latency.py`:

```python
#!/usr/bin/env python3
"""Analyze fleet latency capture artifacts.

Reads `consumer.jsonl` and `publisher/publisher_robot_*.jsonl` from a capture
output directory and reports per-stream latency percentiles, throughput, and
drop rate. Drop rate joins on the (robot_id, suffix, t0_ns) set so duplicate
deliveries (MQTT QoS 1) do not inflate the matched count.

Usage:
    python3 tools/analyze_latency.py latency_artifacts/<run>/
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import statistics
from collections import defaultdict
from typing import Optional


def _read_jsonl(path: str):
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


def _percentile(sorted_vals, q: float) -> float:
    """Nearest-rank percentile on an already-sorted list. q in [0, 1]."""
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    idx = q * (len(sorted_vals) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = idx - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


def analyze(output_dir: str) -> dict:
    consumer_path = os.path.join(output_dir, "consumer.jsonl")
    pub_dir = os.path.join(output_dir, "publisher")

    # Consumer side: latency samples + received t0 sets, keyed by suffix.
    lat_ms_by_suffix = defaultdict(list)
    recv_t0_by_suffix = defaultdict(set)
    t1_min = None
    t1_max = None
    if os.path.exists(consumer_path):
        for rec in _read_jsonl(consumer_path):
            suffix = rec["suffix"]
            lat_ms_by_suffix[suffix].append(rec["latency_ns"] / 1e6)
            recv_t0_by_suffix[suffix].add((rec["robot_id"], rec["t0_ns"]))
            t1 = rec["t1_ns"]
            t1_min = t1 if t1_min is None else min(t1_min, t1)
            t1_max = t1 if t1_max is None else max(t1_max, t1)

    # Publisher side: published t0 sets, keyed by suffix (may be absent).
    pub_t0_by_suffix = defaultdict(set)
    have_pub = os.path.isdir(pub_dir)
    if have_pub:
        for path in sorted(glob.glob(os.path.join(pub_dir, "publisher_robot_*.jsonl"))):
            for rec in _read_jsonl(path):
                pub_t0_by_suffix[rec["suffix"]].add((rec["robot_id"], rec["t0_ns"]))

    window_s = ((t1_max - t1_min) / 1e9) if (t1_min is not None and t1_max is not None and t1_max > t1_min) else None

    by_suffix = {}
    for suffix, samples in sorted(lat_ms_by_suffix.items()):
        s = sorted(samples)
        published: Optional[int] = None
        matched: Optional[int] = None
        drop_rate: Optional[float] = None
        if have_pub and suffix in pub_t0_by_suffix:
            pub_set = pub_t0_by_suffix[suffix]
            published = len(pub_set)
            matched = len(pub_set & recv_t0_by_suffix[suffix])
            drop_rate = (published - matched) / published if published else None
        by_suffix[suffix] = {
            "count": len(s),
            "p50_ms": round(_percentile(s, 0.50), 3),
            "p95_ms": round(_percentile(s, 0.95), 3),
            "p99_ms": round(_percentile(s, 0.99), 3),
            "max_ms": round(s[-1], 3) if s else 0.0,
            "mean_ms": round(statistics.fmean(s), 3) if s else 0.0,
            "throughput_msg_s": round(len(s) / window_s, 1) if window_s else None,
            "published": published,
            "matched": matched,
            "drop_rate": drop_rate,
        }

    total = sum(v["count"] for v in by_suffix.values())
    return {
        "output_dir": output_dir,
        "window_s": round(window_s, 1) if window_s else None,
        "total_received": total,
        "have_publisher_logs": have_pub,
        "by_suffix": by_suffix,
    }


def _print_report(report: dict) -> None:
    print(f"Capture: {report['output_dir']}")
    win = report["window_s"]
    print(f"Window : {win}s   Total received: {report['total_received']:,}")
    if not report["have_publisher_logs"]:
        print("(no publisher logs found — drop rate unavailable)")
    print()
    hdr = (f"{'suffix':<8} {'count':>8} {'p50':>8} {'p95':>8} {'p99':>8} "
           f"{'max':>8} {'msg/s':>8} {'drop%':>7}")
    print(hdr)
    print("-" * len(hdr))
    for suffix, v in report["by_suffix"].items():
        drop = "   n/a" if v["drop_rate"] is None else f"{v['drop_rate'] * 100:6.2f}"
        tput = "   n/a" if v["throughput_msg_s"] is None else f"{v['throughput_msg_s']:8.1f}"
        print(f"{suffix:<8} {v['count']:>8,} {v['p50_ms']:>8.2f} "
              f"{v['p95_ms']:>8.2f} {v['p99_ms']:>8.2f} {v['max_ms']:>8.2f} "
              f"{tput} {drop:>7}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze fleet latency capture artifacts.")
    parser.add_argument("output_dir", help="Capture output directory (contains consumer.jsonl)")
    args = parser.parse_args()
    _print_report(analyze(args.output_dir))


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/maciej/Github/ros2-robot-fleet-demo && python3 -m pytest tests/test_analyze_latency.py -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
cd /home/maciej/Github/ros2-robot-fleet-demo
chmod +x tools/analyze_latency.py
git add tools/analyze_latency.py tests/test_analyze_latency.py
git commit -m "feat(tools): add analyze_latency.py for percentiles and drop rate"
```

---

## Task 5: Orchestration script (`tools/run_latency_capture.sh`)

**Files:**
- Create: `tools/run_latency_capture.sh`
- Test: `tests/test_run_latency_capture.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_run_latency_capture.py`:

```python
"""Smoke tests for run_latency_capture.sh that need no Docker/broker."""
import os
import pathlib
import subprocess

REPO = pathlib.Path(__file__).resolve().parents[1]
SCRIPT = REPO / "tools" / "run_latency_capture.sh"


def test_script_passes_bash_syntax_check():
    r = subprocess.run(["bash", "-n", str(SCRIPT)], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr


def test_missing_bag_path_fails_fast(tmp_path):
    # BAG_PATH points nowhere and no default bag exists under a temp CWD.
    env = dict(os.environ, BAG_PATH=str(tmp_path / "nope"), DURATION="1")
    r = subprocess.run(["bash", str(SCRIPT)], cwd=str(tmp_path),
                       env=env, capture_output=True, text=True)
    assert r.returncode != 0
    assert "metadata.yaml" in (r.stdout + r.stderr) or \
           "BAG" in (r.stdout + r.stderr)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/maciej/Github/ros2-robot-fleet-demo && python3 -m pytest tests/test_run_latency_capture.py -v`
Expected: FAIL — script does not exist yet (`bash -n` returns non-zero "No such file").

- [ ] **Step 3: Write `tools/run_latency_capture.sh`**

Create `tools/run_latency_capture.sh`:

```bash
#!/usr/bin/env bash
# One-shot fleet latency capture.
#
# Runs the documented 3-stage flow so the consumer is listening before any
# robot publishes, captures for a fixed duration, then tears everything down.
# Produces:
#   <OUTPUT_DIR>/consumer.jsonl               (per-message latency)
#   <OUTPUT_DIR>/publisher/publisher_robot_*.jsonl  (per-message publish times)
#
# Env:
#   BAG_PATH    path to a converted ROS 2 bag dir (default: INRAE bag if present)
#   N           number of robots (default: 10)
#   BROKER      kafka (default) | mqtt
#   DURATION    capture seconds (default: 60)
#   OUTPUT_DIR  artifacts dir (default: latency_artifacts/<timestamp>)
#
# Usage:
#   docker build -t ros2-fleet-consumer consumer/        # once
#   BAG_PATH=bags/..._ros2 N=10 BROKER=kafka DURATION=60 ./tools/run_latency_capture.sh
#   python3 tools/analyze_latency.py latency_artifacts/<run>/
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_DIR}"

N="${N:-10}"
BROKER="${BROKER:-kafka}"
DURATION="${DURATION:-60}"
DEFAULT_BAG="bags/rorbots_follower_leader_parcelle_1MONT_ros2"
BAG_PATH="${BAG_PATH:-${DEFAULT_BAG}}"
OUTPUT_DIR="${OUTPUT_DIR:-latency_artifacts/$(date +%Y%m%d_%H%M%S)}"

if [[ ! -f "${BAG_PATH}/metadata.yaml" ]]; then
    echo "ERROR: ${BAG_PATH}/metadata.yaml not found." >&2
    echo "Set BAG_PATH to a converted ROS 2 bag directory (see README)." >&2
    exit 2
fi

if ! docker image inspect ros2-fleet-consumer >/dev/null 2>&1; then
    echo "ERROR: 'ros2-fleet-consumer' image not found. Build it first:" >&2
    echo "  docker build -t ros2-fleet-consumer consumer/" >&2
    exit 3
fi

ABS_OUT="$(mkdir -p "${OUTPUT_DIR}/publisher" && cd "${OUTPUT_DIR}" && pwd)"
CONSUMER_CID=""

cleanup() {
    echo "[capture] tearing down..."
    if [[ -n "${CONSUMER_CID}" ]]; then
        docker stop "${CONSUMER_CID}" >/dev/null 2>&1 || true
    fi
    N="${N}" BROKER="${BROKER}" "${REPO_DIR}/run.sh" --stop || true
}
trap cleanup EXIT INT TERM

echo "============================================="
echo "  Fleet latency capture"
echo "  Robots   : ${N}"
echo "  Broker   : ${BROKER}"
echo "  Duration : ${DURATION}s"
echo "  Bag      : ${BAG_PATH}"
echo "  Output   : ${ABS_OUT}"
echo "============================================="

# Stage 1: brokers.
N="${N}" BROKER="${BROKER}" "${REPO_DIR}/run.sh" --stage brokers

# Stage 2: consumer (listening before robots publish).
echo "[capture] starting consumer..."
CONSUMER_CID="$(docker run -d --network host \
    -v "${ABS_OUT}:/logs" \
    ros2-fleet-consumer --broker "${BROKER}" \
    --log-file /logs/consumer.jsonl --stats-only)"
sleep 3

# Stage 3: robots, with publisher logging into <OUTPUT_DIR>/publisher.
LATENCY_LOG_DIR="${ABS_OUT}/publisher" \
    N="${N}" BROKER="${BROKER}" BAG_PATH="${BAG_PATH}" \
    "${REPO_DIR}/run.sh" --stage robots

echo "[capture] capturing for ${DURATION}s..."
sleep "${DURATION}"

# Stop consumer first so it flushes; teardown handled by trap.
docker stop "${CONSUMER_CID}" >/dev/null 2>&1 || true
CONSUMER_CID=""

CONSUMER_LINES=$(wc -l < "${ABS_OUT}/consumer.jsonl" 2>/dev/null || echo 0)
PUB_LINES=$(cat "${ABS_OUT}"/publisher/publisher_robot_*.jsonl 2>/dev/null | wc -l || echo 0)

echo ""
echo "[capture] done."
echo "  consumer.jsonl : ${CONSUMER_LINES} records"
echo "  publisher/*    : ${PUB_LINES} records"
echo ""
echo "  Analyze with:"
echo "    python3 tools/analyze_latency.py ${ABS_OUT}/"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/maciej/Github/ros2-robot-fleet-demo && python3 -m pytest tests/test_run_latency_capture.py -v`
Expected: PASS (2 passed). The missing-bag test exits before any Docker call.

- [ ] **Step 5: Commit**

```bash
cd /home/maciej/Github/ros2-robot-fleet-demo
chmod +x tools/run_latency_capture.sh
git add tools/run_latency_capture.sh tests/test_run_latency_capture.py
git commit -m "feat(tools): add run_latency_capture.sh 3-stage orchestration"
```

---

## Task 6: README documentation

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add a "Latency capture" section**

In `README.md`, after the "## Trajectory recording and plotting" section
(before "## Decoding CDR in Python"), insert:

```markdown
## Latency capture

Record per-message end-to-end latency (ROS publish → broker → consumer) to
JSONL and summarize it. Latency is `t1_ns − header.stamp`, where each robot
stamps `header.stamp = time.time_ns()` at publish time.

```bash
# 0. Build the consumer image once
docker build -t ros2-fleet-consumer consumer/

# 1. Run a 60 s capture with 10 robots on Kafka
BAG_PATH=bags/rorbots_follower_leader_parcelle_1MONT_ros2 \
    N=10 BROKER=kafka DURATION=60 ./tools/run_latency_capture.sh

# 2. Analyze the artifacts the run prints the path to
python3 tools/analyze_latency.py latency_artifacts/<run>/
```

Artifacts written to `latency_artifacts/<timestamp>/`:

| File | Contents |
|------|----------|
| `consumer.jsonl` | one record per received message: `robot_id, suffix, topic, t0_ns, t1_ns, latency_ns, payload_bytes` |
| `publisher/publisher_robot_<id>.jsonl` | one record per published message: `robot_id, suffix, topic, t0_ns` |

`analyze_latency.py` joins the two sides on the `(robot_id, suffix, t0_ns)`
set — so MQTT QoS-1 duplicate deliveries do not inflate the match count — and
prints per-stream p50/p95/p99/max latency, throughput, and drop rate.

The orchestrator runs the 3-stage flow (brokers → consumer → robots) so the
consumer never misses early messages, captures for `DURATION` seconds, then
tears the fleet down (also on Ctrl+C).
```

- [ ] **Step 2: Verify the markdown renders (no broken fences)**

Run: `cd /home/maciej/Github/ros2-robot-fleet-demo && grep -c '^```' README.md`
Expected: an even number (all code fences balanced).

- [ ] **Step 3: Commit**

```bash
cd /home/maciej/Github/ros2-robot-fleet-demo
git add README.md
git commit -m "docs: document fleet latency capture workflow"
```

---

## Final verification

- [ ] **Run the full test suite**

Run: `cd /home/maciej/Github/ros2-robot-fleet-demo && python3 -m pytest tests/ -v`
Expected: all tests pass (existing `test_bag_looper.py`, `test_consume_health.py`
plus the four new test files).

- [ ] **Confirm backward compatibility of `gen_fleet.sh`**

Run:
```bash
cd /home/maciej/Github/ros2-robot-fleet-demo
BROKER=kafka MSG_TYPE=multi bash gen_fleet.sh 2 /tmp/_a.yml
git stash >/dev/null 2>&1 || true   # only if needed to compare against main
```
Expected: generated file contains no `LATENCY_LOG_DIR` when the env var is
unset (already covered by `tests/test_gen_fleet_latency.py::test_no_injection_when_unset`).

---

## Self-Review notes (addressed)

- **Spec coverage:** consume.py logging (T1), robot_replay.py logging (T2),
  gen_fleet.sh mount (T3), analyzer (T4), orchestration (T5), README (T6) — all
  six spec components have tasks.
- **Drop-rate robustness:** spec called for joining on `(robot_id, suffix,
  t0_ns)`; analyzer uses `(robot_id, t0_ns)` sets per suffix, which dedupes
  duplicate deliveries — the test asserts a duplicate t0 does not inflate
  `matched`.
- **Type consistency:** `PublisherLatencyLogger(log_dir, robot_id)` /
  `.record(suffix, topic, t0_ns)` / `.close()` used identically in T2 and the
  test. `_log_latency(robot_id, suffix, topic, t0_ns, t1_ns, latency_ns,
  payload_bytes)` signature identical in T1 implementation, wiring, and test.
- **No placeholders:** every code step contains complete code.
```
