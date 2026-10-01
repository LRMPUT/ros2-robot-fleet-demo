# Design: Health Monitoring + `.env` Command Cache

**Date:** 2026-05-27
**Status:** Approved

---

## 1. `.env` Command Cache

### Goal

Allow users to run `./run.sh` without re-typing env vars every time.

### Design

`run.sh` reads `.env` from the project root at startup. Shell env vars always win — `.env` only fills in vars that are not already set.

Loading priority (highest first):

```
shell environment  >  .env file  >  run.sh hardcoded defaults
```

### Implementation in `run.sh`

Add near the top, after `set -euo pipefail`, before variable declarations:

```bash
if [[ -f "${SCRIPT_DIR}/.env" ]]; then
    while IFS= read -r line || [[ -n "$line" ]]; do
        [[ "$line" =~ ^[[:space:]]*# ]] && continue   # skip comments
        [[ "$line" =~ ^[[:space:]]*$ ]] && continue   # skip blanks
        key="${line%%=*}"
        [[ -v "$key" ]] && continue                   # shell already set it
        export "$line"
    done < "${SCRIPT_DIR}/.env"
fi
```

The `[[ -v "$key" ]]` check ensures any var already exported by the shell is never overwritten.

### `.env.example` (committed to repo)

```bash
# Copy to .env and fill in your values.
# Shell environment variables always override .env.
N=10
BROKER=mqtt
BAG_PATH=/path/to/your/ros2_bag
PAYLOAD_FORMAT=json
# TOPOLOGY=shared
# MSG_TYPE=multi
# RATE_HZ=10
```

`.env` is added to `.gitignore`.

---

## 2. Health Monitoring

### Coverage

| Scenario | Solution |
|---|---|
| No data flowing (bag loops fine but broker down) | Consumer no-data alert (§2c) |
| Message rate drop / stall detection | Per-robot silence detection (§2b) |
| Per-robot "silence" alert | Per-robot silence detection (§2b) |
| MQTT broker health check | Docker healthcheck (§2a) |
| Consumer-side "no messages received" timeout | Consumer no-data alert (§2c) |
| Empty bag (topic exists but zero messages) | BagLooper empty-bag check (§2e) |

### 2a. MQTT Broker Healthcheck

**File:** `docker-compose.mqtt.yml`

Add `healthcheck` to the `broker` service so `docker ps` reports `(healthy)`:

```yaml
healthcheck:
  test: ["CMD", "mosquitto_pub", "-h", "localhost", "-t", "_health", "-n", "-q", "0"]
  interval: 5s
  timeout: 3s
  retries: 5
```

`mosquitto_pub -n` sends a zero-byte message — a lightweight probe that does not require a payload.

### 2b. Per-Robot Silence Detection

**File:** `consumer/consume.py`

#### New state

```python
_last_seen: dict[str, float] = {}    # robot_id → time.monotonic() of last message
_warned_silent: set[str] = set()     # robots currently in warned-silent state
```

`_last_seen` is updated in `_record()` alongside the existing counters.

#### Silence threshold

Default: `10.0` seconds. Exposed as `--silence-threshold SECONDS` CLI arg.

#### Warning logic (runs inside the existing 1-second stats loop)

```
for each robot_id in _last_seen:
    if now - _last_seen[robot_id] > silence_threshold:
        if robot_id not in _warned_silent:
            print(f"[WARNING] robot_{robot_id} silent for {elapsed:.0f}s")
            _warned_silent.add(robot_id)
    else:
        _warned_silent.discard(robot_id)   # robot recovered, clear warning
```

Warning fires once when silence begins; clears automatically when data resumes.

### 2c. No-Data Alert

**File:** `consumer/consume.py`

If `_total_msgs == 0` and `time.monotonic() - _start_time > 15.0`, print once:

```
[WARNING] No messages received — is the fleet running and broker reachable?
```

Guarded by a `_warned_no_data: bool` flag so it prints exactly once.

This catches two scenarios: broker down before consumer starts, and fleet never launched.

### 2d. `--silence-threshold` CLI arg

```
--silence-threshold SECONDS   seconds of silence before a robot is flagged (default: 10)
```

### 2e. Empty Bag Check in `BagLooper`

**File:** `robot_replay.py`

Currently `BagLooper` raises `RuntimeError` when a topic type is absent entirely. But if the topic exists with zero messages, `has_next()` returns `False` immediately after open, leading to `read_next()` on an empty reader — a cryptic internal exception.

Fix in `BagLooper.__next__`: after reopening on EOF, check `has_next()` again and raise a clear error if the bag is still empty:

```python
def __next__(self):
    if self._reader is None:
        raise RuntimeError("BagLooper is permanently broken after failed re-open")
    if not self._reader.has_next():
        self._open_reader()
        if not self._reader.has_next():
            raise RuntimeError(
                f"Bag {self._bag_path} contains no messages of type {self._topic_type_str}"
            )
    _topic, data, _t = self._reader.read_next()
    return deserialize_message(data, self._msg_class)
```

The container exits with a non-zero code, Docker marks it unhealthy, and the error message clearly identifies the bag and type.

---

## Files Changed

| File | Change |
|---|---|
| `run.sh` | Source `.env` at startup; shell env wins |
| `.env.example` | New file, committed |
| `.gitignore` | Add `.env` |
| `docker-compose.mqtt.yml` | Add broker healthcheck |
| `consumer/consume.py` | Add `_last_seen`, silence detection, no-data alert, `--silence-threshold` |
| `robot_replay.py` | Empty-bag check in `BagLooper.__next__` |

---

## Out of Scope

- Auto-restart of stalled robots
- Health monitoring for Kafka broker (already has healthcheck)
- Per-robot health HTTP endpoint
