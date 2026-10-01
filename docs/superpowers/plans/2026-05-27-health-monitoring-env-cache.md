# Health Monitoring + `.env` Command Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add pipeline health monitoring (empty-bag detection, MQTT broker healthcheck, per-robot silence alerts, no-data alert) and a `.env` file so `./run.sh` can be run without re-typing env vars.

**Architecture:** Four independent changes: (1) a two-line guard in `BagLooper.__next__` catches empty bags early; (2) a Docker healthcheck probe is added to the MQTT broker service; (3) `run.sh` reads `.env` at startup, skipping vars already in the shell; (4) `consume.py` tracks per-robot last-seen timestamps and prints warnings in its existing 1-second stats loop.

**Tech Stack:** Python 3, bash, Docker Compose, paho-mqtt, pytest

---

## File Map

| File | Change |
|---|---|
| `robot_replay.py` | Add empty-bag guard in `BagLooper.__next__` |
| `docker-compose.mqtt.yml` | Add `healthcheck` block to `broker` service |
| `run.sh` | Source `.env` at startup; shell env wins |
| `.env.example` | New file — template users copy to `.env` |
| `consumer/consume.py` | Add `_last_seen`, silence detection, no-data alert, `--silence-threshold` |
| `tests/test_bag_looper.py` | New — unit tests for empty-bag check |
| `tests/test_consume_health.py` | New — unit tests for health monitoring logic |

---

## Task 1: Empty-bag guard in `BagLooper`

**Files:**
- Modify: `robot_replay.py` (lines 113–119, `BagLooper.__next__`)
- Create: `tests/test_bag_looper.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_bag_looper.py`:

```python
import sys
from unittest.mock import MagicMock, patch, PropertyMock

# Stub all ROS2 imports so robot_replay can be imported without a ROS install
for mod in [
    "rosbag2_py", "rclpy", "rclpy.node", "rclpy.serialization",
    "rclpy.executors", "rosidl_runtime_py", "rosidl_runtime_py.utilities",
    "nav_msgs", "nav_msgs.msg", "sensor_msgs", "sensor_msgs.msg",
]:
    sys.modules.setdefault(mod, MagicMock())

import pytest
import robot_replay  # noqa: E402  (import after stubs)
from robot_replay import BagLooper


def _make_reader(has_messages: bool):
    """Return a mock SequentialReader that looks full or empty."""
    reader = MagicMock()
    reader.get_all_topics_and_types.return_value = [
        MagicMock(name="/robot_1/gnss", type="sensor_msgs/msg/NavSatFix")
    ]
    reader.has_next.return_value = has_messages
    if has_messages:
        reader.read_next.return_value = ("/robot_1/gnss", b"\x00" * 8, 0)
    return reader


@patch("robot_replay.rosbag2_py")
@patch("robot_replay.get_message", return_value=MagicMock())
@patch("robot_replay.deserialize_message", return_value=MagicMock())
def test_empty_bag_raises_clear_error(mock_deser, mock_get_msg, mock_ros2):
    mock_ros2.SequentialReader.return_value = _make_reader(has_messages=False)
    mock_ros2.StorageOptions = MagicMock()
    mock_ros2.ConverterOptions = MagicMock()
    mock_ros2.StorageFilter = MagicMock()

    looper = BagLooper("/fake/bag", "sensor_msgs/msg/NavSatFix")
    with pytest.raises(RuntimeError, match="contains no messages"):
        next(looper)


@patch("robot_replay.rosbag2_py")
@patch("robot_replay.get_message", return_value=MagicMock())
@patch("robot_replay.deserialize_message", return_value=MagicMock())
def test_non_empty_bag_returns_message(mock_deser, mock_get_msg, mock_ros2):
    mock_ros2.SequentialReader.return_value = _make_reader(has_messages=True)
    mock_ros2.StorageOptions = MagicMock()
    mock_ros2.ConverterOptions = MagicMock()
    mock_ros2.StorageFilter = MagicMock()

    looper = BagLooper("/fake/bag", "sensor_msgs/msg/NavSatFix")
    msg = next(looper)
    assert msg is not None
```

- [ ] **Step 2: Run test to confirm it fails**

```bash
cd /home/maciej/Github/ros2-robot-fleet-demo
pip install pytest -q
pytest tests/test_bag_looper.py -v
```

Expected: `test_empty_bag_raises_clear_error` **FAILS** (no RuntimeError raised yet).

- [ ] **Step 3: Add the empty-bag guard to `BagLooper.__next__`**

In `robot_replay.py`, replace:

```python
    def __next__(self):
        if self._reader is None:
            raise RuntimeError("BagLooper is permanently broken after failed re-open")
        if not self._reader.has_next():
            self._open_reader()
        _topic, data, _t = self._reader.read_next()
        return deserialize_message(data, self._msg_class)
```

With:

```python
    def __next__(self):
        if self._reader is None:
            raise RuntimeError("BagLooper is permanently broken after failed re-open")
        if not self._reader.has_next():
            self._open_reader()
            if not self._reader.has_next():
                raise RuntimeError(
                    f"Bag {self._bag_path} contains no messages of type "
                    f"{self._topic_type_str}"
                )
        _topic, data, _t = self._reader.read_next()
        return deserialize_message(data, self._msg_class)
```

- [ ] **Step 4: Run tests — both must pass**

```bash
pytest tests/test_bag_looper.py -v
```

Expected:
```
tests/test_bag_looper.py::test_empty_bag_raises_clear_error PASSED
tests/test_bag_looper.py::test_non_empty_bag_returns_message PASSED
```

- [ ] **Step 5: Commit**

```bash
git add robot_replay.py tests/test_bag_looper.py
git commit -m "feat(replay): raise clear error when bag has topic but zero messages"
```

---

## Task 2: MQTT broker Docker healthcheck

**Files:**
- Modify: `docker-compose.mqtt.yml`

- [ ] **Step 1: Add healthcheck to `docker-compose.mqtt.yml`**

Replace:

```yaml
services:
  broker:
    image: eclipse-mosquitto:2.0
    ports:
      - "1883:1883"
    volumes:
      - ./mosquitto.conf:/mosquitto/config/mosquitto.conf:ro
```

With:

```yaml
services:
  broker:
    image: eclipse-mosquitto:2.0
    ports:
      - "1883:1883"
    volumes:
      - ./mosquitto.conf:/mosquitto/config/mosquitto.conf:ro
    healthcheck:
      test: ["CMD", "mosquitto_pub", "-h", "localhost", "-t", "_health", "-n", "-q", "0"]
      interval: 5s
      timeout: 3s
      retries: 5
```

- [ ] **Step 2: Verify the healthcheck works when fleet is running**

Start the MQTT fleet (or broker only) and confirm `(healthy)` appears:

```bash
N=2 BROKER=mqtt BAG_PATH=./bags/rorbots_follower_leader_parcelle_1MONT_ros2 ./run.sh --stage brokers
sleep 10
docker ps --format "table {{.Names}}\t{{.Status}}" | grep broker
```

Expected output contains: `ros2-robot-fleet-demo-broker-1   Up Xs (healthy)`

```bash
./run.sh --stop
```

- [ ] **Step 3: Commit**

```bash
git add docker-compose.mqtt.yml
git commit -m "feat(mqtt): add broker healthcheck via mosquitto_pub probe"
```

---

## Task 3: `.env` command cache in `run.sh`

**Files:**
- Modify: `run.sh`
- Create: `.env.example`
- Note: `.env` is already in `.gitignore` (confirmed present)

- [ ] **Step 1: Add `.env` loader to `run.sh`**

In `run.sh`, after the line `cd "${SCRIPT_DIR}"` and before `N="${N:-10}"`, insert:

```bash
# Load .env if present; shell-set vars always take precedence.
if [[ -f "${SCRIPT_DIR}/.env" ]]; then
    while IFS= read -r line || [[ -n "$line" ]]; do
        [[ "$line" =~ ^[[:space:]]*# ]] && continue
        [[ "$line" =~ ^[[:space:]]*$ ]] && continue
        key="${line%%=*}"
        [[ -v "$key" ]] && continue
        export "$line"
    done < "${SCRIPT_DIR}/.env"
fi
```

The full top of `run.sh` should now look like:

```bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

# Load .env if present; shell-set vars always take precedence.
if [[ -f "${SCRIPT_DIR}/.env" ]]; then
    while IFS= read -r line || [[ -n "$line" ]]; do
        [[ "$line" =~ ^[[:space:]]*# ]] && continue
        [[ "$line" =~ ^[[:space:]]*$ ]] && continue
        key="${line%%=*}"
        [[ -v "$key" ]] && continue
        export "$line"
    done < "${SCRIPT_DIR}/.env"
fi

N="${N:-10}"
BROKER="${BROKER:-kafka}"
```

- [ ] **Step 2: Create `.env.example`**

Create `/home/maciej/Github/ros2-robot-fleet-demo/.env.example` with:

```bash
# Copy this file to .env and fill in your values.
# Shell environment variables always override .env.
N=10
BROKER=mqtt
BAG_PATH=/path/to/your/ros2_bag
PAYLOAD_FORMAT=json
# TOPOLOGY=shared
# MSG_TYPE=multi
# RATE_HZ=10
```

- [ ] **Step 3: Smoke-test `.env` loading**

```bash
# Create a temp .env and confirm run.sh picks it up (--stop exits fast, no broker needed)
echo "N=3" > /tmp/test_dotenv_n.env
cp /tmp/test_dotenv_n.env /home/maciej/Github/ros2-robot-fleet-demo/.env

# run.sh --stop reads N from .env; check the printed header
N= BROKER=mqtt ./run.sh --stop 2>&1 | head -10
# Expected: "Robots   : 3"  (from .env, not the empty N= override)
# Shell N= is exported as empty string, so [[ -v N ]] is true → .env value ignored.
# Actually shell empty export wins over .env. Verify with unset:
unset N
./run.sh --stop 2>&1 | head -8
# Expected: "Robots   : 3"

rm /home/maciej/Github/ros2-robot-fleet-demo/.env
```

Expected header line: `  Robots   : 3`

- [ ] **Step 4: Confirm shell var overrides `.env`**

```bash
echo "N=3" > .env
N=7 ./run.sh --stop 2>&1 | grep Robots
# Expected: "  Robots   : 7"   (shell N=7 wins over .env N=3)
rm .env
```

- [ ] **Step 5: Commit**

```bash
git add run.sh .env.example
git commit -m "feat(run): load .env as default env vars; shell env always wins"
```

---

## Task 4: Per-robot silence detection and no-data alert in `consume.py`

**Files:**
- Modify: `consumer/consume.py`
- Create: `tests/test_consume_health.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_consume_health.py`:

```python
"""Tests for health monitoring logic in consume.py.

consume.py has no ROS2 dependencies in its health-monitoring paths,
so these tests run without a broker or ROS install.
"""
import sys
from unittest.mock import MagicMock

# Stub broker libraries so consume.py can be imported standalone
sys.modules.setdefault("confluent_kafka", MagicMock())
sys.modules.setdefault("paho", MagicMock())
sys.modules.setdefault("paho.mqtt", MagicMock())
sys.modules.setdefault("paho.mqtt.client", MagicMock())

import time
import pytest
import consumer.consume as consume


def _reset():
    """Reset all module-level health state between tests."""
    consume._last_seen.clear()
    consume._warned_silent.clear()
    consume._total_msgs = 0
    consume._total_bytes = 0
    consume._warned_no_data = False
    consume._start_time = time.monotonic()
    consume._counts.clear()
    consume._bytes.clear()


# --- _record tracks last_seen ---

def test_record_updates_last_seen():
    _reset()
    consume._record("ros2/robot_3/gnss", 100, robot_id=3)
    assert 3 in consume._last_seen


def test_record_without_robot_id_does_not_update_last_seen():
    _reset()
    consume._record("ros2/robot_3/gnss", 100)
    assert consume._last_seen == {}


# --- silence detection ---

def test_silence_warning_added_when_robot_goes_quiet(capsys):
    _reset()
    past = time.monotonic() - 15.0
    consume._last_seen[5] = past

    consume._check_health(silence_threshold=10.0)

    captured = capsys.readouterr()
    assert "[WARNING]" in captured.out
    assert "robot_5" in captured.out
    assert 5 in consume._warned_silent


def test_silence_warning_not_repeated_for_same_robot(capsys):
    _reset()
    past = time.monotonic() - 15.0
    consume._last_seen[5] = past

    consume._check_health(silence_threshold=10.0)
    consume._check_health(silence_threshold=10.0)

    captured = capsys.readouterr()
    assert captured.out.count("[WARNING]") == 1  # fires exactly once, not twice


def test_silence_warning_clears_when_robot_recovers(capsys):
    _reset()
    consume._last_seen[5] = time.monotonic() - 15.0
    consume._check_health(silence_threshold=10.0)   # trigger warning
    consume._last_seen[5] = time.monotonic()          # robot recovered
    consume._check_health(silence_threshold=10.0)   # should clear

    assert 5 not in consume._warned_silent


# --- no-data alert ---

def test_no_data_alert_fires_after_15s(capsys):
    _reset()
    consume._start_time = time.monotonic() - 20.0   # pretend 20s have passed
    consume._total_msgs = 0

    consume._check_health(silence_threshold=10.0)

    captured = capsys.readouterr()
    assert "No messages received" in captured.out
    assert consume._warned_no_data is True


def test_no_data_alert_does_not_fire_before_15s(capsys):
    _reset()
    consume._start_time = time.monotonic() - 5.0    # only 5s elapsed
    consume._total_msgs = 0

    consume._check_health(silence_threshold=10.0)

    captured = capsys.readouterr()
    assert "No messages received" not in captured.out


def test_no_data_alert_does_not_fire_if_messages_received(capsys):
    _reset()
    consume._start_time = time.monotonic() - 20.0
    consume._total_msgs = 42                         # messages are flowing

    consume._check_health(silence_threshold=10.0)

    captured = capsys.readouterr()
    assert "No messages received" not in captured.out


def test_no_data_alert_fires_only_once(capsys):
    _reset()
    consume._start_time = time.monotonic() - 20.0
    consume._total_msgs = 0

    consume._check_health(silence_threshold=10.0)
    consume._check_health(silence_threshold=10.0)

    captured = capsys.readouterr()
    # First call prints it; second call sees _warned_no_data=True and skips.
    assert captured.out.count("No messages received") == 1
```

- [ ] **Step 2: Run tests to confirm they all fail**

```bash
pytest tests/test_consume_health.py -v
```

Expected: all tests **FAIL** (`_last_seen`, `_check_health`, `_warned_no_data` not defined yet).

- [ ] **Step 3: Add new global state to `consume.py`**

After the existing globals block (after `_total_bytes = 0`, around line 36), add:

```python
_last_seen: dict[int, float] = {}    # robot_id → time.monotonic() of last message
_warned_silent: set[int] = set()     # robots currently in warned-silent state
_start_time: float = time.monotonic()
_warned_no_data: bool = False
```

- [ ] **Step 4: Update `_record` to accept and store `robot_id`**

Replace:

```python
def _record(topic: str, n_bytes: int) -> None:
    global _total_msgs, _total_bytes
    with _lock:
        _counts[topic] += 1
        _bytes[topic] += n_bytes
        _total_msgs += 1
        _total_bytes += n_bytes
```

With:

```python
def _record(topic: str, n_bytes: int, robot_id: Optional[int] = None) -> None:
    global _total_msgs, _total_bytes
    with _lock:
        _counts[topic] += 1
        _bytes[topic] += n_bytes
        _total_msgs += 1
        _total_bytes += n_bytes
        if robot_id is not None:
            _last_seen[robot_id] = time.monotonic()
```

- [ ] **Step 5: Add `_check_health` function**

Add this new function after `_record`:

```python
def _check_health(silence_threshold: float = 10.0) -> None:
    global _warned_no_data
    now = time.monotonic()

    with _lock:
        total_m = _total_msgs
        snap_last_seen = dict(_last_seen)

    if not _warned_no_data and total_m == 0 and (now - _start_time) > 15.0:
        print("\n[WARNING] No messages received — is the fleet running and broker reachable?",
              flush=True)
        _warned_no_data = True

    for robot_id, last_t in snap_last_seen.items():
        elapsed = now - last_t
        if elapsed > silence_threshold:
            if robot_id not in _warned_silent:
                print(f"\n[WARNING] robot_{robot_id} silent for {elapsed:.0f}s", flush=True)
                _warned_silent.add(robot_id)
        else:
            _warned_silent.discard(robot_id)
```

- [ ] **Step 6: Update `_stats_loop` to call `_check_health` and accept `silence_threshold`**

Replace the `_stats_loop` signature and body:

```python
def _stats_loop(interval: float = 1.0, stats_only: bool = False,
                silence_threshold: float = 10.0) -> None:
    t_prev = time.monotonic()
    while not _stop.is_set():
        time.sleep(interval)
        now = time.monotonic()
        dt = now - t_prev
        t_prev = now
        with _lock:
            snap_counts = dict(_counts)
            snap_bytes  = dict(_bytes)
            total_m = _total_msgs
            _counts.clear()
            _bytes.clear()
        _check_health(silence_threshold)
        total_rate = sum(snap_counts.values()) / dt
        total_kb   = sum(snap_bytes.values()) / dt / 1024
        robots_seen = {_parse_kafka_topic(t) or _parse_mqtt_topic(t)
                       for t in snap_counts} - {None}
        robot_ids = sorted({r for r, _ in robots_seen if r is not None})
        line = (f"[stats]  {total_rate:6.0f} msg/s  {total_kb:7.1f} KB/s"
                f"  robots={len(robot_ids)}  total={total_m:,}")
        if stats_only:
            print(f"\r{line}", end="", flush=True)
        else:
            print(line, flush=True)
```

- [ ] **Step 7: Pass `robot_id` in `consume_kafka`**

In `consume_kafka`, replace:

```python
        _record(msg.topic(), len(msg.value()))
```

With:

```python
        _record(msg.topic(), len(msg.value()), robot_id=robot_id)
```

- [ ] **Step 8: Pass `robot_id` in `consume_mqtt`**

In `consume_mqtt`'s `on_message` callback, replace:

```python
        _record(msg.topic, len(msg.payload))
```

With:

```python
        _record(msg.topic, len(msg.payload), robot_id=robot_id)
```

- [ ] **Step 9: Add `--silence-threshold` arg and wire it to `_stats_loop`**

In `main`, add after `--format`:

```python
    parser.add_argument(
        "--silence-threshold", type=float, default=10.0, metavar="SECONDS",
        help="seconds of robot silence before a warning is printed (default: 10)",
    )
```

Update the stats thread creation:

```python
    stats_thread = threading.Thread(
        target=_stats_loop,
        kwargs={"stats_only": args.stats_only, "silence_threshold": args.silence_threshold},
        daemon=True,
    )
```

- [ ] **Step 10: Run all tests — all must pass**

```bash
pytest tests/test_consume_health.py tests/test_bag_looper.py -v
```

Expected: all 10 tests **PASS**.

- [ ] **Step 11: Rebuild the consumer Docker image**

```bash
cd /home/maciej/Github/ros2-robot-fleet-demo
docker build -t ros2-fleet-consumer -f consumer/Dockerfile .
```

- [ ] **Step 12: Smoke-test with a running fleet**

```bash
N=10 BROKER=mqtt PAYLOAD_FORMAT=json \
  BAG_PATH=./bags/rorbots_follower_leader_parcelle_1MONT_ros2 \
  ./run.sh

# In a second terminal — watch for [WARNING] if a robot goes silent
docker run --rm --network host ros2-fleet-consumer --broker mqtt --stats-only --silence-threshold 15
```

- [ ] **Step 13: Commit**

```bash
git add consumer/consume.py tests/test_consume_health.py
git commit -m "feat(consumer): add per-robot silence alerts and no-data warning"
```
