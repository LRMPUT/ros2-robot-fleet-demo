# Multi-stage Latency Measurement — Design

**Date:** 2026-05-28
**Branch:** `feat/latency-capture`
**Repo:** `ros2-robot-fleet-demo`
**Scope of this spec:** Phase 1 (Kafka) only. Phase 2 (MQTT) is described as a
documented follow-up and is **not** implemented here.

## Goal

Decompose the end-to-end latency captured by the fleet latency tooling into
stages, so we can see *where* time is spent (ROS publish → sink handoff vs. sink
→ broker → consume), not just the total. Three timestamps:

| Stamp | Meaning | Source |
|-------|---------|--------|
| `t0_ns` | publish from bag replay | ROS `header.stamp` (already set by `robot_replay.py`) |
| `t1_ns` | **sink-produce** — the moment the sink hands the message to its broker client | Kafka record `CreateTime` via `msg.timestamp()` (ms→ns). **MQTT: `null` in Phase 1** |
| `t2_ns` | consume | consumer wall-clock at receive (the value previously logged as `t1_ns`) |

All three stamps are produced on the **same host** (`network_mode: host`, shared
kernel clock): `t0_ns` and `t1_ns` come from the robot container (which runs both
`robot_replay.py` and its own sink in the edge topology), and `t2_ns` from the
consumer container. No clock-skew correction is needed.

Derived per-record stages (milliseconds):

- **ingest** = `(t1_ns − t0_ns)` — ROS publish → sink handoff (DDS transport +
  serialization inside the robot container).
- **transport** = `(t2_ns − t1_ns)` — sink produce → broker → consumer.
- **e2e** = `(t2_ns − t0_ns)` — total (unchanged headline metric).

## Why Kafka needs no sink change

`kafka_sink` already stamps each Kafka record's `CreateTime` with its own
produce wall-clock (`now_ns / 1'000'000`, milliseconds). The default Kafka
broker config is `CreateTime` (the broker does not overwrite it), so a consumer
reading `msg.timestamp()` gets exactly the sink-produce time. The consumer
currently ignores it; Phase 1 simply reads and logs it.

Precision: `CreateTime` is **milliseconds**. For sub-millisecond ingest stages
this is coarse (±0.5 ms granularity), which is accepted for Phase 1 — it still
distinguishes sub-ms from multi-ms ingest. A nanosecond Kafka header was
considered and rejected for Phase 1 to avoid a dispatcher-repo change.

## Why MQTT is deferred to Phase 2

MQTT subscribers never observe broker-receive time (the protocol has no broker
timestamp), and `mosquitto_sink` currently embeds nothing. Carrying a
sink-produce time requires a `mosquitto_sink` change in the `ros2_kafka_dispatcher`
repo plus a GHCR image rebuild. In Phase 1, MQTT records log `t1_ns = null`, and
the analyzer reports `ingest`/`transport` as `n/a` for them (e2e is unaffected).

## JSONL schema change

`consumer.jsonl` record changes from:

```json
{"robot_id", "suffix", "topic", "t0_ns", "t1_ns", "latency_ns", "payload_bytes"}
```

to:

```json
{"robot_id", "suffix", "topic", "t0_ns", "t1_ns", "t2_ns", "latency_ns", "payload_bytes"}
```

- `t0_ns` — publish (unchanged; this is the publisher↔consumer join key and is
  **not** renamed, so drop-rate accounting is unaffected).
- `t1_ns` — **new meaning**: sink-produce time (nullable). Integer ns for Kafka
  (`msg.timestamp()` ms × 1e6), `null` for MQTT in Phase 1.
- `t2_ns` — **renamed** from the old `t1_ns`: consumer receive time.
- `latency_ns` — unchanged semantics: `t2_ns − t0_ns` (end-to-end).

`publisher_robot_<id>.jsonl` is unchanged (`{robot_id, suffix, topic, t0_ns}`).

Old capture artifacts under `latency_artifacts/` are disposable (gitignored) and
will be regenerated; no migration is needed.

## Components

### 1. `consumer/consume.py`

- `_log_latency` signature gains a `t1_ns` (sink) parameter; the existing
  receive-time argument is recorded as `t2_ns`. The record dict becomes the
  8-field schema above. `latency_ns` is computed as `t2_ns − t0_ns`.
- `consume_kafka`: after computing the receive time, read
  `ts_type, ts_ms = msg.timestamp()`. Compute
  `t1_ns = ts_ms * 1_000_000 if (ts_ms is not None and ts_ms > 0) else None`.
  (Kafka returns `-1`/`0` when unavailable; map those to `None`.) Pass `t1_ns`
  (sink) and the receive time (`t2_ns`) to `_log_latency`.
- `consume_mqtt`: pass `t1_ns = None` (Phase 1).
- The human-readable stdout line is unchanged (still shows e2e latency).

### 2. `tools/analyze_latency.py`

- Read `t1_ns` and `t2_ns` from each consumer record. e2e latency continues to
  come from `latency_ns`.
- For records where `t1_ns is not None`, accumulate per-suffix samples:
  - `ingest_ms = (t1_ns − t0_ns) / 1e6`
  - `transport_ms = (t2_ns − t1_ns) / 1e6`
- `by_suffix[suffix]` gains: `ingest_p50_ms`, `transport_p50_ms`,
  `staged_count` (number of records that had a non-null `t1_ns`). When
  `staged_count == 0` (e.g. MQTT Phase 1), these report `None`.
- `_print_report` adds two columns — `ingest` and `transport` (ms) — printed as
  `n/a` when `None`. Existing e2e columns (p50/p95/p99/max/throughput/drop%)
  stay.

## Data flow

```
robot_replay.py            kafka_sink (same container)        consumer
  header.stamp=t0  --DDS-->  CreateTime=t1 (ms)  --Kafka-->  recv=t2
        |                          |                            |
        +--- ingest=t1-t0 ---------+--- transport=t2-t1 --------+
        +------------------- e2e = t2-t0 -------------------------+
```

(MQTT Phase 1: `t1` is `null`; only e2e is reported.)

## Testing

- **`consume.py`:** unit test that `_log_latency` writes the 8-field record with
  a numeric `t1_ns` and correct `t2_ns`/`latency_ns`; and a second case with
  `t1_ns=None` that still writes a well-formed record (e2e present, `t1_ns`
  null). Reuse the existing MagicMock-stub pattern.
- **`analyze_latency.py`:** unit test on synthetic `consumer.jsonl` with all
  three stamps → asserts `ingest_p50_ms`, `transport_p50_ms`, and e2e
  percentiles; plus a record with `t1_ns=None` → `ingest`/`transport` report
  `None` while e2e is still computed.
- **Regression:** update the existing `tests/test_consume_logging.py` and
  `tests/test_run_latency_capture_integration.py` for the new field
  (`t2_ns` rename + added `t1_ns`).
- **End-to-end (manual, documented):** a short Kafka run shows non-`n/a`
  ingest/transport columns; an MQTT run shows `n/a` for them.

## Out of scope (Phase 2, separate spec)

- `mosquitto_sink` (dispatcher repo): connect with MQTT v5 and publish a user
  property `t1_ns` = sink-produce wall-clock ns; rebuild the GHCR image.
- Consumer MQTT path: connect with MQTT v5 and read the `t1_ns` user property to
  populate `t1_ns` for MQTT records.
- Nanosecond-precision Kafka `t1_ns` via a record header (only if ms proves too
  coarse in practice).
