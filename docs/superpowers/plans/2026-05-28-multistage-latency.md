# Multi-stage Latency (Phase 1, Kafka) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decompose the fleet's end-to-end latency into `ingest` (publish→sink) and `transport` (sink→broker→consume) stages by logging a third timestamp (`t1_ns` = Kafka record CreateTime) and renaming the old consume field to `t2_ns`.

**Architecture:** Two fleet-repo files change. `consumer/consume.py` reads Kafka `msg.timestamp()` (the sink-produce CreateTime, ms→ns) as `t1_ns`, records consume time as `t2_ns`, and logs `null` for `t1_ns` on MQTT. `tools/analyze_latency.py` computes ingest/transport per record (all three stamps live in one consumer record) and adds two table columns. No dispatcher-repo change, no Docker image rebuild.

**Tech Stack:** Python 3 stdlib (`json`, `statistics`), `confluent_kafka` (Message.timestamp()), pytest with MagicMock-stubbed broker libs.

---

## File Structure

- **Modify** `consumer/consume.py` — `_log_latency` gains a sink `t1_ns` param and a `t2_ns` (consume) param; `consume_kafka` reads `msg.timestamp()`; `consume_mqtt` passes `t1_ns=None`.
- **Modify** `tools/analyze_latency.py` — `analyze()` reads `t1_ns`/`t2_ns`, computes ingest/transport, uses `t2_ns` for the throughput window; `_print_report` adds `ingest`/`transp` columns.
- **Modify** `tests/test_consume_logging.py` — update for the 8-field record; add a null-sink case.
- **Modify** `tests/test_analyze_latency.py` — rename `t1_ns`→`t2_ns` + add sink `t1_ns` in fixtures; assert ingest/transport; add a null-sink test.

`tests/test_run_latency_capture_integration.py` asserts on the generated compose file, not the JSONL schema, so it needs **no** change.

---

## Task 1: Consumer — log sink stamp `t1_ns` and rename consume to `t2_ns`

**Files:**
- Modify: `consumer/consume.py`
- Test: `tests/test_consume_logging.py`

- [ ] **Step 1: Update the existing tests and add a null-sink case (failing first)**

Replace the entire body of `tests/test_consume_logging.py` with:

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
    consume._log_latency(1, "gnss", "ros2.robot_1.gnss", 100, 150, 200, 100, 42)


def test_log_latency_writes_one_jsonl_record():
    _reset_log()
    buf = io.StringIO()
    consume._log_fh = buf
    try:
        consume._log_latency(3, "gnss", "ros2.robot_3.gnss",
                             1000, 1200, 1500, 500, 142)
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
        "t1_ns": 1200,
        "t2_ns": 1500,
        "latency_ns": 500,
        "payload_bytes": 142,
    }


def test_log_latency_null_sink_stamp():
    _reset_log()
    buf = io.StringIO()
    consume._log_fh = buf
    try:
        # MQTT (Phase 1) has no sink stamp: t1_ns is None -> JSON null.
        consume._log_latency(2, "odom", "ros2/robot_2/odom",
                             1000, None, 1500, 500, 80)
    finally:
        consume._log_fh = None

    rec = json.loads(buf.getvalue().strip())
    assert rec["t1_ns"] is None
    assert rec["t0_ns"] == 1000
    assert rec["t2_ns"] == 1500
    assert rec["latency_ns"] == 500
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /home/maciej/Github/ros2-robot-fleet-demo && python3 -m pytest tests/test_consume_logging.py -v`
Expected: FAIL — `_log_latency` currently takes 7 args, the tests now pass 8 (TypeError), and the expected record lacks `t2_ns`.

- [ ] **Step 3: Update `_log_latency` signature and record**

In `consumer/consume.py`, replace the `_log_latency` function with:

```python
def _log_latency(robot_id: int, suffix: str, topic: str,
                 t0_ns: int, t1_ns, t2_ns: int, latency_ns: int,
                 payload_bytes: int) -> None:
    """Append one JSONL latency record. No-op when logging is disabled.

    t1_ns is the sink-produce stamp (ns) or None when unavailable (MQTT).
    t2_ns is the consumer receive stamp. latency_ns is end-to-end (t2 - t0).
    """
    # _log_fh is set once in main() before threads start; safe to read without lock.
    if _log_fh is None:
        return
    rec = _json.dumps({
        "robot_id": robot_id,
        "suffix": suffix,
        "topic": topic,
        "t0_ns": t0_ns,
        "t1_ns": t1_ns,
        "t2_ns": t2_ns,
        "latency_ns": latency_ns,
        "payload_bytes": payload_bytes,
    }, separators=(",", ":"))
    with _log_lock:
        _log_fh.write(rec + "\n")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /home/maciej/Github/ros2-robot-fleet-demo && python3 -m pytest tests/test_consume_logging.py -v`
Expected: PASS (3 passed).

- [ ] **Step 5: Wire the Kafka sink stamp into `consume_kafka`**

In `consumer/consume.py`, in `consume_kafka`, replace this block:

```python
        t1_ns = time.time_ns()
        t0_ns = _decode(msg.value(), suffix, fmt, msg.headers())
        if t0_ns is None:
            continue
        lat_ms = (t1_ns - t0_ns) / 1e6
        _record(msg.topic(), len(msg.value()), robot_id=robot_id)
        _log_latency(robot_id, suffix, msg.topic(),
                     t0_ns, t1_ns, t1_ns - t0_ns, len(msg.value()))
        _print_line(robot_id, msg.topic(), suffix, lat_ms, len(msg.value()), stats_only)
```

with:

```python
        t2_ns = time.time_ns()
        t0_ns = _decode(msg.value(), suffix, fmt, msg.headers())
        if t0_ns is None:
            continue
        # Kafka record CreateTime is the sink-produce wall-clock (ms). -1/0 = unset.
        _ts_type, ts_ms = msg.timestamp()
        t1_ns = ts_ms * 1_000_000 if (ts_ms is not None and ts_ms > 0) else None
        lat_ms = (t2_ns - t0_ns) / 1e6
        _record(msg.topic(), len(msg.value()), robot_id=robot_id)
        _log_latency(robot_id, suffix, msg.topic(),
                     t0_ns, t1_ns, t2_ns, t2_ns - t0_ns, len(msg.value()))
        _print_line(robot_id, msg.topic(), suffix, lat_ms, len(msg.value()), stats_only)
```

- [ ] **Step 6: Wire the MQTT path (no sink stamp in Phase 1)**

In `consumer/consume.py`, in `consume_mqtt`'s `on_message`, replace this block:

```python
        t1_ns = time.time_ns()
        t0_ns = _decode(msg.payload, suffix, fmt)
        if t0_ns is None:
            return
        lat_ms = (t1_ns - t0_ns) / 1e6
        _record(msg.topic, len(msg.payload), robot_id=robot_id)
        _log_latency(robot_id, suffix, msg.topic,
                     t0_ns, t1_ns, t1_ns - t0_ns, len(msg.payload))
        _print_line(robot_id, msg.topic, suffix, lat_ms, len(msg.payload), stats_only)
```

with:

```python
        t2_ns = time.time_ns()
        t0_ns = _decode(msg.payload, suffix, fmt)
        if t0_ns is None:
            return
        lat_ms = (t2_ns - t0_ns) / 1e6
        _record(msg.topic, len(msg.payload), robot_id=robot_id)
        # MQTT (Phase 1) has no broker/sink timestamp visible to subscribers.
        _log_latency(robot_id, suffix, msg.topic,
                     t0_ns, None, t2_ns, t2_ns - t0_ns, len(msg.payload))
        _print_line(robot_id, msg.topic, suffix, lat_ms, len(msg.payload), stats_only)
```

- [ ] **Step 7: Run the consumer test suite**

Run: `cd /home/maciej/Github/ros2-robot-fleet-demo && python3 -m pytest tests/test_consume_logging.py tests/test_consume_health.py -v`
Expected: PASS (all tests).

- [ ] **Step 8: Commit**

```bash
cd /home/maciej/Github/ros2-robot-fleet-demo
git add consumer/consume.py tests/test_consume_logging.py
git commit -m "feat(consumer): log Kafka sink stamp as t1_ns; rename consume to t2_ns"
```

---

## Task 2: Analyzer — ingest/transport stages

**Files:**
- Modify: `tools/analyze_latency.py`
- Test: `tests/test_analyze_latency.py`

- [ ] **Step 1: Update fixtures and add stage assertions (failing first)**

Replace the entire body of `tests/test_analyze_latency.py` with:

```python
"""Tests for the latency analyzer's join + stage/percentile math."""
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


def test_analyze_computes_stages_drop_rate_and_percentiles(tmp_path):
    pub_dir = tmp_path / "publisher"
    pub_dir.mkdir()
    # robot 1 gnss published 3 messages (t0 = 100, 200, 300)
    _write_jsonl(pub_dir / "publisher_robot_1.jsonl", [
        {"robot_id": 1, "suffix": "gnss", "topic": "/robot_1/gnss", "t0_ns": 100},
        {"robot_id": 1, "suffix": "gnss", "topic": "/robot_1/gnss", "t0_ns": 200},
        {"robot_id": 1, "suffix": "gnss", "topic": "/robot_1/gnss", "t0_ns": 300},
    ])
    # Consumer received 2 distinct (t0 = 100, 300); 200 dropped; 100 delivered
    # twice (duplicate must not inflate matched). Each record carries the sink
    # stamp t1_ns so ingest = t1-t0 and transport = t2-t1 are computable.
    #   rec1: t0=100  t1=500100   t2=1000100  -> ingest 0.5ms transport 0.5ms e2e 1.0ms
    #   rec2: t0=100  t1=600100   t2=1100100  -> ingest 0.6ms transport 0.5ms e2e 1.1ms
    #   rec3: t0=300  t1=1350300  t2=2700300  -> ingest 1.35ms transport 1.35ms e2e 2.7ms
    _write_jsonl(tmp_path / "consumer.jsonl", [
        {"robot_id": 1, "suffix": "gnss", "topic": "ros2.robot_1.gnss",
         "t0_ns": 100, "t1_ns": 500100, "t2_ns": 1000100,
         "latency_ns": 1000000, "payload_bytes": 10},
        {"robot_id": 1, "suffix": "gnss", "topic": "ros2.robot_1.gnss",
         "t0_ns": 100, "t1_ns": 600100, "t2_ns": 1100100,
         "latency_ns": 1100000, "payload_bytes": 10},
        {"robot_id": 1, "suffix": "gnss", "topic": "ros2.robot_1.gnss",
         "t0_ns": 300, "t1_ns": 1350300, "t2_ns": 2700300,
         "latency_ns": 2700000, "payload_bytes": 10},
    ])

    report = al.analyze(str(tmp_path))
    gnss = report["by_suffix"]["gnss"]
    # Drop accounting (join on (robot_id, t0_ns) set, dedup duplicate t0=100).
    assert gnss["published"] == 3
    assert gnss["matched"] == 2
    assert gnss["drop_rate"] == 1 / 3
    # End-to-end percentiles from latency_ns -> [1.0, 1.1, 2.7] ms.
    assert gnss["count"] == 3
    assert gnss["p50_ms"] == 1.1
    # Stage breakdown.
    assert gnss["staged_count"] == 3
    assert gnss["ingest_p50_ms"] == 0.6       # median of [0.5, 0.6, 1.35]
    assert gnss["transport_p50_ms"] == 0.5    # median of [0.5, 0.5, 1.35]


def test_analyze_null_sink_reports_stages_na(tmp_path):
    # MQTT Phase 1: t1_ns is null -> stages n/a but e2e still computed.
    _write_jsonl(tmp_path / "consumer.jsonl", [
        {"robot_id": 2, "suffix": "odom", "topic": "ros2/robot_2/odom",
         "t0_ns": 5, "t1_ns": None, "t2_ns": 1000005,
         "latency_ns": 1000000, "payload_bytes": 8},
    ])
    report = al.analyze(str(tmp_path))
    odom = report["by_suffix"]["odom"]
    assert odom["count"] == 1
    assert odom["staged_count"] == 0
    assert odom["ingest_p50_ms"] is None
    assert odom["transport_p50_ms"] is None
    # No publisher dir -> drop accounting degrades.
    assert odom["published"] is None
    assert odom["drop_rate"] is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /home/maciej/Github/ros2-robot-fleet-demo && python3 -m pytest tests/test_analyze_latency.py -v`
Expected: FAIL — `analyze()` reads `rec["t1_ns"]` as the window time and the result lacks `staged_count`/`ingest_p50_ms`/`transport_p50_ms` (KeyError), and the consumer records no longer have a numeric `t1_ns` for the window.

- [ ] **Step 3: Update the consumer loop to read `t2_ns` and collect stage samples**

In `tools/analyze_latency.py`, in `analyze()`, replace this block:

```python
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
```

with:

```python
    # Consumer side: latency samples, stage samples, and received t0 sets,
    # keyed by suffix.
    lat_ms_by_suffix = defaultdict(list)
    ingest_ms_by_suffix = defaultdict(list)
    transport_ms_by_suffix = defaultdict(list)
    recv_t0_by_suffix = defaultdict(set)
    t2_min = None
    t2_max = None
    if os.path.exists(consumer_path):
        for rec in _read_jsonl(consumer_path):
            suffix = rec["suffix"]
            lat_ms_by_suffix[suffix].append(rec["latency_ns"] / 1e6)
            recv_t0_by_suffix[suffix].add((rec["robot_id"], rec["t0_ns"]))
            t2 = rec["t2_ns"]
            t2_min = t2 if t2_min is None else min(t2_min, t2)
            t2_max = t2 if t2_max is None else max(t2_max, t2)
            # Sink stamp t1_ns is None for MQTT (Phase 1); skip its stages.
            t1 = rec.get("t1_ns")
            if t1 is not None:
                ingest_ms_by_suffix[suffix].append((t1 - rec["t0_ns"]) / 1e6)
                transport_ms_by_suffix[suffix].append((t2 - t1) / 1e6)
```

- [ ] **Step 4: Update the window calc to use `t2`**

In `tools/analyze_latency.py`, replace:

```python
    window_s = ((t1_max - t1_min) / 1e9) if (t1_min is not None and t1_max is not None and t1_max > t1_min) else None
```

with:

```python
    window_s = ((t2_max - t2_min) / 1e9) if (t2_min is not None and t2_max is not None and t2_max > t2_min) else None
```

- [ ] **Step 5: Add stage stats to the per-suffix dict**

In `tools/analyze_latency.py`, replace the `by_suffix[suffix] = { ... }` assignment block:

```python
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
```

with:

```python
        ingest = sorted(ingest_ms_by_suffix.get(suffix, []))
        transport = sorted(transport_ms_by_suffix.get(suffix, []))
        by_suffix[suffix] = {
            "count": len(s),
            "p50_ms": round(_percentile(s, 0.50), 3),
            "p95_ms": round(_percentile(s, 0.95), 3),
            "p99_ms": round(_percentile(s, 0.99), 3),
            "max_ms": round(s[-1], 3) if s else 0.0,
            "mean_ms": round(statistics.fmean(s), 3) if s else 0.0,
            "throughput_msg_s": round(len(s) / window_s, 1) if window_s else None,
            "staged_count": len(ingest),
            "ingest_p50_ms": round(_percentile(ingest, 0.50), 3) if ingest else None,
            "transport_p50_ms": round(_percentile(transport, 0.50), 3) if transport else None,
            "published": published,
            "matched": matched,
            "drop_rate": drop_rate,
        }
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cd /home/maciej/Github/ros2-robot-fleet-demo && python3 -m pytest tests/test_analyze_latency.py -v`
Expected: PASS (2 passed).

- [ ] **Step 7: Add `ingest`/`transp` columns to the printed table**

In `tools/analyze_latency.py`, in `_print_report`, replace the header line:

```python
    hdr = (f"{'suffix':<8} {'count':>8} {'p50':>8} {'p95':>8} {'p99':>8} "
           f"{'max':>8} {'msg/s':>8} {'drop%':>7}")
```

with:

```python
    hdr = (f"{'suffix':<8} {'count':>8} {'p50':>8} {'p95':>8} {'p99':>8} "
           f"{'max':>8} {'msg/s':>8} {'drop%':>7} {'ingest':>8} {'transp':>8}")
```

and replace the per-row print block:

```python
    for suffix, v in report["by_suffix"].items():
        drop = "   n/a" if v["drop_rate"] is None else f"{v['drop_rate'] * 100:6.2f}"
        tput = "   n/a" if v["throughput_msg_s"] is None else f"{v['throughput_msg_s']:8.1f}"
        print(f"{suffix:<8} {v['count']:>8,} {v['p50_ms']:>8.2f} "
              f"{v['p95_ms']:>8.2f} {v['p99_ms']:>8.2f} {v['max_ms']:>8.2f} "
              f"{tput} {drop:>7}")
```

with:

```python
    for suffix, v in report["by_suffix"].items():
        drop = "n/a" if v["drop_rate"] is None else f"{v['drop_rate'] * 100:.2f}"
        tput = "n/a" if v["throughput_msg_s"] is None else f"{v['throughput_msg_s']:.1f}"
        ing = "n/a" if v["ingest_p50_ms"] is None else f"{v['ingest_p50_ms']:.2f}"
        tra = "n/a" if v["transport_p50_ms"] is None else f"{v['transport_p50_ms']:.2f}"
        print(f"{suffix:<8} {v['count']:>8,} {v['p50_ms']:>8.2f} "
              f"{v['p95_ms']:>8.2f} {v['p99_ms']:>8.2f} {v['max_ms']:>8.2f} "
              f"{tput:>8} {drop:>7} {ing:>8} {tra:>8}")
```

(The `ingest`/`transp` columns are stage **p50** latencies in ms; the p50/p95/p99/max columns remain end-to-end.)

- [ ] **Step 8: Run the analyzer tests once more**

Run: `cd /home/maciej/Github/ros2-robot-fleet-demo && python3 -m pytest tests/test_analyze_latency.py -v`
Expected: PASS (2 passed).

- [ ] **Step 9: Commit**

```bash
cd /home/maciej/Github/ros2-robot-fleet-demo
git add tools/analyze_latency.py tests/test_analyze_latency.py
git commit -m "feat(tools): analyzer reports ingest/transport stages from t1_ns/t2_ns"
```

---

## Final verification

- [ ] **Run the full suite**

Run: `cd /home/maciej/Github/ros2-robot-fleet-demo && python3 -m pytest tests/ -v`
Expected: all tests pass (the two updated files plus the unchanged `test_bag_looper.py`, `test_consume_health.py`, `test_gen_fleet_latency.py`, `test_publisher_logging.py`, `test_run_latency_capture.py`, `test_run_latency_capture_integration.py`).

---

## Self-Review notes (addressed)

- **Spec coverage:** schema change `t0_ns`/`t1_ns`(sink)/`t2_ns`(consume)/`latency_ns` (Task 1); Kafka reads CreateTime ms→ns, MQTT null (Task 1 steps 5–6); analyzer ingest/transport + window-on-`t2` + columns (Task 2); null-sink degrade (both test files). `test_run_latency_capture_integration.py` needs no change (asserts compose, not schema) — noted in File Structure.
- **Type consistency:** `_log_latency(robot_id, suffix, topic, t0_ns, t1_ns, t2_ns, latency_ns, payload_bytes)` used identically in the function def, both call sites, and all three logging tests. Analyzer keys `staged_count`/`ingest_p50_ms`/`transport_p50_ms` used identically in `analyze()`, `_print_report`, and both analyzer tests.
- **No placeholders:** every code step contains complete code.
```
