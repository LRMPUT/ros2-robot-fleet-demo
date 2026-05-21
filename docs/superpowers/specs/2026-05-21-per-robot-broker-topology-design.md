# Per-Robot Broker Topology — Design Spec

**Date:** 2026-05-21
**Branch target:** main

## Goal

Add an optional `TOPOLOGY=per-robot` mode where each simulated robot gets its
own dedicated MQTT broker instance, simulating an edge deployment where each
robot carries a local broker.

## Context

The existing topology (`TOPOLOGY=shared`, default) routes all robots to a
single broker:

```
robot_1 → mosquitto_sink → ─┐
robot_2 → mosquitto_sink → ─┤→  one Mosquitto broker :1883
robot_N → mosquitto_sink → ─┘
```

The new topology assigns one broker per robot:

```
robot_1 → mosquitto_sink → broker_1 :1883
robot_2 → mosquitto_sink → broker_2 :1884
robot_N → mosquitto_sink → broker_N :1882+N
```

## Scope

- MQTT only. Kafka KRaft requires non-trivial per-instance cluster config
  (`node_id`, `cluster_id`, voter lists) — out of scope.
- `TOPOLOGY=per-robot` is validated at runtime; `TOPOLOGY=per-robot BROKER=kafka`
  exits with a clear error.

## Port Allocation

```
broker_N listens on host port: 1882 + N
```

| Robot | Host port | Internal port |
|-------|-----------|---------------|
| 1     | 1883      | 1883          |
| 2     | 1884      | 1883          |
| 3     | 1885      | 1883          |
| N     | 1882+N    | 1883          |

All containers use `network_mode: host`. Each robot connects to
`localhost:<host-port>` via the new `BROKER_PORT` env var.

## Files Changed

### `edge_entrypoint.sh`

Add `BROKER_PORT` parameter (currently hardcoded to `1883`):

```bash
: "${BROKER_PORT:=1883}"
```

Pass it to the mosquitto_sink params:

```yaml
mqtt.broker_port: ${BROKER_PORT}
```

### `gen_fleet.sh`

Add `TOPOLOGY` env var (default: `shared`).

In the robot service loop, branch on `TOPOLOGY`:

- `shared` — existing behaviour: `BROKER_HOST=localhost`, `BROKER_PORT=1883`,
  `depends_on: broker`.
- `per-robot` — for each robot `i`:
  1. Emit a `broker_N` service (Mosquitto image, `ports: "1882+i:1883"`,
     same `mosquitto.conf` volume).
  2. Emit the robot service with `BROKER_HOST=localhost`,
     `BROKER_PORT=1882+i`, `depends_on: broker_N`.

The generated compose file is self-contained when `TOPOLOGY=per-robot`
(no external broker service needed).

### `run.sh`

Pass `TOPOLOGY` to `gen_fleet.sh`.

Skip `docker-compose.mqtt.yml` when `TOPOLOGY=per-robot` to avoid starting a
redundant singleton broker:

```bash
if [[ "${TOPOLOGY:-shared}" == "per-robot" ]]; then
    COMPOSE_ARGS=(-f "${FLEET_COMPOSE}")
else
    COMPOSE_ARGS=(-f "docker-compose.${BROKER}.yml" -f "${FLEET_COMPOSE}")
fi
```

Validate early:

```bash
if [[ "${TOPOLOGY:-shared}" == "per-robot" && "${BROKER}" != "mqtt" ]]; then
    echo "ERROR: TOPOLOGY=per-robot is only supported with BROKER=mqtt" >&2
    exit 1
fi
```

### Fleet file naming

Include topology in the filename so `--stop` finds the right file:

```
.fleet/robots_mqtt_5_per-robot.yml   # TOPOLOGY=per-robot
.fleet/robots_mqtt_5.yml             # TOPOLOGY=shared (unchanged)
```

## Usage

```bash
# 5 robots, each with its own Mosquitto broker
TOPOLOGY=per-robot N=5 BROKER=mqtt BAG_PATH=/path/to/bag ./run.sh

# Stop
TOPOLOGY=per-robot N=5 BROKER=mqtt ./run.sh --stop

# Subscribe to robot_2's broker
mosquitto_sub -h localhost -p 1884 -t 'ros2/#' -v
```

## Consumer Impact

`consumer/consume.py` connects to a single broker. In `per-robot` mode,
users must run one consumer instance per broker or aggregate manually.
No changes to `consume.py` in this spec.

## Out of Scope

- Kafka per-robot brokers
- Automatic consumer aggregation across N brokers
- Dynamic port allocation (ports are deterministic: `1882 + robot_id`)
