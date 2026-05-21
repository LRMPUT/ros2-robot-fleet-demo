# Per-Robot Broker Topology Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `TOPOLOGY=per-robot` mode where each MQTT robot gets its own dedicated Mosquitto broker instance, plus add `mqtt.message_key` and `BROKER_PORT` support to `edge_entrypoint.sh`.

**Architecture:** Three shell scripts are modified — `edge_entrypoint.sh` gains `BROKER_PORT` + `mqtt.message_key`; `gen_fleet.sh` gains `TOPOLOGY` branching that emits inline `broker_N` services; `run.sh` gains topology-aware broker startup and fleet file naming. No new files needed.

**Tech Stack:** Bash, Docker Compose v2, eclipse-mosquitto:2.0

---

### Task 1: `edge_entrypoint.sh` — add `BROKER_PORT` and `mqtt.message_key`

**Files:**
- Modify: `edge_entrypoint.sh`

- [ ] **Step 1: Add `BROKER_PORT` env var declaration**

  In the env var block (after `MQTT_QOS` line, before the `PAYLOAD_FORMAT` block), add:

  ```bash
  : "${BROKER_PORT:=1883}"
  ```

  The full block should look like:
  ```bash
  : "${ROBOT_ID:?ROBOT_ID env var is required}"
  : "${SINK_KIND:=kafka}"
  : "${BAG_PATH:?BAG_PATH env var is required}"
  : "${BROKER_HOST:=localhost}"
  : "${MSG_TYPE:=multi}"
  : "${RATE_HZ:=10}"
  : "${MQTT_QOS:=1}"
  : "${BROKER_PORT:=1883}"
  ```

- [ ] **Step 2: Update MQTT params section to use `BROKER_PORT` and add `mqtt.message_key`**

  Find the `mqtt)` case in the `case "${SINK_KIND}"` block. Replace:
  ```bash
      mqtt.broker_host: "${BROKER_HOST}"
      mqtt.broker_port: 1883
      mqtt.client_id: "mosquitto_sink_${ROBOT_ID}"
      mqtt.topic_prefix: ros2
      mqtt.qos: ${MQTT_QOS}
      mqtt.payload_format: "${PAYLOAD_FORMAT}"
      metrics.enabled: false
  ```
  With:
  ```bash
      mqtt.broker_host: "${BROKER_HOST}"
      mqtt.broker_port: ${BROKER_PORT}
      mqtt.client_id: "mosquitto_sink_${ROBOT_ID}"
      mqtt.topic_prefix: ros2
      mqtt.qos: ${MQTT_QOS}
      mqtt.payload_format: "${PAYLOAD_FORMAT}"
      mqtt.message_key: "robot_${ROBOT_ID}"
      metrics.enabled: false
  ```

- [ ] **Step 3: Verify params file renders correctly (dry run)**

  ```bash
  ROBOT_ID=3 SINK_KIND=mqtt BAG_PATH=/dev/null BROKER_PORT=1885 \
    bash -c 'source edge_entrypoint.sh 2>/dev/null || true' 2>&1 | grep -A20 "params:"
  ```

  Expected: params block shows `mqtt.broker_port: 1885` and `mqtt.message_key: "robot_3"`.
  (The script will fail after params print because ROS is not available — that's fine.)

- [ ] **Step 4: Commit**

  ```bash
  git add edge_entrypoint.sh
  git commit -m "feat(edge): add BROKER_PORT and mqtt.message_key params"
  ```

---

### Task 2: `gen_fleet.sh` — add `TOPOLOGY` support

**Files:**
- Modify: `gen_fleet.sh`

- [ ] **Step 1: Add `TOPOLOGY` env var and validation at top of script**

  After the existing env var block (after the `PAYLOAD_FORMAT` if/else block), add:

  ```bash
  TOPOLOGY="${TOPOLOGY:-shared}"

  if [[ "${TOPOLOGY}" == "per-robot" && "${BROKER}" != "mqtt" ]]; then
      echo "ERROR: TOPOLOGY=per-robot is only supported with BROKER=mqtt" >&2
      exit 1
  fi
  if [[ "${TOPOLOGY}" != "shared" && "${TOPOLOGY}" != "per-robot" ]]; then
      echo "ERROR: unknown TOPOLOGY=${TOPOLOGY}; expected 'shared' or 'per-robot'" >&2
      exit 1
  fi
  ```

- [ ] **Step 2: Replace the robot service loop with topology-branching version**

  Replace the entire `for ((i = 1; i <= N; i++)); do ... done` block with:

  ```bash
  for ((i = 1; i <= N; i++)); do
      if [[ "${TOPOLOGY}" == "per-robot" ]]; then
          PORT=$((1882 + i))
          # Emit dedicated broker for this robot
          cat >> "${OUT}" <<EOF
    broker_${i}:
      image: eclipse-mosquitto:2.0
      network_mode: host
      ports:
        - "${PORT}:1883"
      volumes:
        - ./mosquitto.conf:/mosquitto/config/mosquitto.conf:ro
  EOF
          # Emit robot pointing at its own broker
          cat >> "${OUT}" <<EOF
    robot_${i}:
      image: ${IMAGE}
      network_mode: host
      ipc: host
      environment:
        ROBOT_ID: "${i}"
        BAG_PATH: "/data/bag"
        RATE_HZ: "${RATE_HZ}"
        MSG_TYPE: "${MSG_TYPE}"
        SINK_KIND: "mqtt"
        BROKER_HOST: "localhost"
        BROKER_PORT: "${PORT}"
        MQTT_QOS: "1"
        PAYLOAD_FORMAT: "${PAYLOAD_FORMAT}"
      volumes:
        - "\${BAG_PATH:?BAG_PATH is required}:/data/bag:ro"
        - ./edge_entrypoint.sh:/usr/local/bin/edge_entrypoint.sh:ro
        - ./robot_replay.py:/app/robot_replay.py:ro
      entrypoint: ["/usr/local/bin/edge_entrypoint.sh"]
      depends_on:
        - broker_${i}
      restart: "no"
  EOF
      else
          # Existing shared-broker behaviour
          cat >> "${OUT}" <<EOF
    robot_${i}:
      image: ${IMAGE}
      network_mode: host
      ipc: host
      environment:
        ROBOT_ID: "${i}"
        BAG_PATH: "/data/bag"
        RATE_HZ: "${RATE_HZ}"
        MSG_TYPE: "${MSG_TYPE}"
        SINK_KIND: "${BROKER}"
        BROKER_HOST: "localhost"
        BROKER_PORT: "1883"
        MQTT_QOS: "1"
        PAYLOAD_FORMAT: "${PAYLOAD_FORMAT}"
      volumes:
        - "\${BAG_PATH:?BAG_PATH is required}:/data/bag:ro"
        - ./edge_entrypoint.sh:/usr/local/bin/edge_entrypoint.sh:ro
        - ./robot_replay.py:/app/robot_replay.py:ro
      entrypoint: ["/usr/local/bin/edge_entrypoint.sh"]
      depends_on:
        - broker
      restart: "no"
  EOF
      fi
  done
  ```

- [ ] **Step 3: Update the log line at the end of the script**

  Replace:
  ```bash
  echo "[gen_fleet] wrote ${N} robot services (broker=${BROKER}, msg_type=${MSG_TYPE}, payload=${PAYLOAD_FORMAT}) → ${OUT}"
  ```
  With:
  ```bash
  echo "[gen_fleet] wrote ${N} robot services (broker=${BROKER}, topology=${TOPOLOGY}, msg_type=${MSG_TYPE}, payload=${PAYLOAD_FORMAT}) → ${OUT}"
  ```

- [ ] **Step 4: Test shared topology — generated YAML is valid and unchanged in shape**

  ```bash
  BROKER=mqtt N=2 bash gen_fleet.sh 2 /tmp/test_shared.yml
  cat /tmp/test_shared.yml
  docker compose -f docker-compose.mqtt.yml -f /tmp/test_shared.yml config --quiet
  ```

  Expected: no errors; file has `robot_1` and `robot_2`, both with `depends_on: broker`, `BROKER_PORT: "1883"`. No `broker_1`/`broker_2` services.

- [ ] **Step 5: Test per-robot topology — generated YAML has inline brokers**

  ```bash
  TOPOLOGY=per-robot BROKER=mqtt N=3 bash gen_fleet.sh 3 /tmp/test_per_robot.yml
  cat /tmp/test_per_robot.yml
  docker compose -f /tmp/test_per_robot.yml config --quiet
  ```

  Expected: no errors; file contains `broker_1`, `broker_2`, `broker_3` with ports `1883`, `1884`, `1885`; robots have `BROKER_PORT: "1883"`, `"1884"`, `"1885"` respectively; each robot `depends_on` its own broker.

- [ ] **Step 6: Test validation — Kafka + per-robot is rejected**

  ```bash
  TOPOLOGY=per-robot BROKER=kafka N=2 bash gen_fleet.sh 2 /tmp/should_fail.yml
  echo "exit code: $?"
  ```

  Expected: prints `ERROR: TOPOLOGY=per-robot is only supported with BROKER=mqtt`, exit code 1.

- [ ] **Step 7: Commit**

  ```bash
  git add gen_fleet.sh
  git commit -m "feat(gen_fleet): add TOPOLOGY=per-robot mode with inline broker services"
  ```

---

### Task 3: `run.sh` — topology-aware startup and fleet file naming

**Files:**
- Modify: `run.sh`

- [ ] **Step 1: Add `TOPOLOGY` to env var block and header comment**

  Add `TOPOLOGY` to the optional env block at the top of `run.sh`. Replace the existing variable block:

  ```bash
  N="${N:-10}"
  BROKER="${BROKER:-kafka}"
  MSG_TYPE="${MSG_TYPE:-multi}"
  RATE_HZ="${RATE_HZ:-10}"
  ```

  With:

  ```bash
  N="${N:-10}"
  BROKER="${BROKER:-kafka}"
  MSG_TYPE="${MSG_TYPE:-multi}"
  RATE_HZ="${RATE_HZ:-10}"
  TOPOLOGY="${TOPOLOGY:-shared}"
  ```

  Also add `TOPOLOGY` to the header comment block at the top of the script. Find the `# Optional env:` section and add:

  ```bash
  #   TOPOLOGY   — shared (default) | per-robot (MQTT only: one broker per robot)
  ```

- [ ] **Step 2: Add early validation for TOPOLOGY + BROKER combination**

  Add after the `TOPOLOGY=` line:

  ```bash
  if [[ "${TOPOLOGY}" == "per-robot" && "${BROKER}" != "mqtt" ]]; then
      echo "ERROR: TOPOLOGY=per-robot is only supported with BROKER=mqtt" >&2
      exit 1
  fi
  ```

- [ ] **Step 3: Update fleet filename and COMPOSE_ARGS to be topology-aware**

  Replace:
  ```bash
  FLEET_COMPOSE="${FLEET_DIR}/robots_${BROKER}_${N}.yml"
  COMPOSE_ARGS=(-f "docker-compose.${BROKER}.yml" -f "${FLEET_COMPOSE}")
  ```

  With:
  ```bash
  if [[ "${TOPOLOGY}" == "per-robot" ]]; then
      FLEET_COMPOSE="${FLEET_DIR}/robots_${BROKER}_${N}_per-robot.yml"
      COMPOSE_ARGS=(-f "${FLEET_COMPOSE}")
  else
      FLEET_COMPOSE="${FLEET_DIR}/robots_${BROKER}_${N}.yml"
      COMPOSE_ARGS=(-f "docker-compose.${BROKER}.yml" -f "${FLEET_COMPOSE}")
  fi
  ```

- [ ] **Step 4: Update the `stop_fleet` fallback to use correct base file**

  The `stop_fleet` function's fallback `docker compose -f "docker-compose.${BROKER}.yml" down` runs when the fleet file is missing. For `per-robot` this would fail (no base file). Replace the fallback line:

  ```bash
      else
          # Fallback: tear down by project name (broker + any orphaned containers).
          docker compose -f "docker-compose.${BROKER}.yml" down -v --remove-orphans 2>&1 | tail -3 || true
      fi
  ```

  With:

  ```bash
      else
          if [[ "${TOPOLOGY}" == "per-robot" ]]; then
              docker compose -p "ros2-robot-fleet-demo" down -v --remove-orphans 2>&1 | tail -3 || true
          else
              docker compose -f "docker-compose.${BROKER}.yml" down -v --remove-orphans 2>&1 | tail -3 || true
          fi
      fi
  ```

- [ ] **Step 5: Update `gen_fleet.sh` invocation to pass `TOPOLOGY`**

  Replace:
  ```bash
  BROKER="${BROKER}" MSG_TYPE="${MSG_TYPE}" RATE_HZ="${RATE_HZ}" \
      "${SCRIPT_DIR}/gen_fleet.sh" "${N}" "${FLEET_COMPOSE}"
  ```

  With:
  ```bash
  BROKER="${BROKER}" MSG_TYPE="${MSG_TYPE}" RATE_HZ="${RATE_HZ}" TOPOLOGY="${TOPOLOGY}" \
      "${SCRIPT_DIR}/gen_fleet.sh" "${N}" "${FLEET_COMPOSE}"
  ```

- [ ] **Step 6: Update broker startup to handle per-robot mode**

  Replace:
  ```bash
  # 2. Start broker.
  echo "[fleet] starting ${BROKER} broker..."
  NUM_ROBOTS="${N}" docker compose "${COMPOSE_ARGS[@]}" up -d broker
  sleep 6
  ```

  With:
  ```bash
  # 2. Start broker(s).
  if [[ "${TOPOLOGY}" == "per-robot" ]]; then
      echo "[fleet] starting ${N} per-robot brokers..."
      broker_services=()
      for ((i=1; i<=N; i++)); do broker_services+=("broker_${i}"); done
      docker compose "${COMPOSE_ARGS[@]}" up -d "${broker_services[@]}"
  else
      echo "[fleet] starting ${BROKER} broker..."
      docker compose "${COMPOSE_ARGS[@]}" up -d broker
  fi
  sleep 6
  ```

- [ ] **Step 7: Update the print banner to show `TOPOLOGY`**

  Replace:
  ```bash
  echo "============================================="
  echo "  ROS 2 Robot Fleet"
  echo "  Robots : ${N}"
  echo "  Broker : ${BROKER}"
  echo "  Topics : ${MSG_TYPE}"
  echo "  Bag    : ${BAG_PATH}"
  echo "============================================="
  ```

  With:
  ```bash
  echo "============================================="
  echo "  ROS 2 Robot Fleet"
  echo "  Robots   : ${N}"
  echo "  Broker   : ${BROKER}"
  echo "  Topology : ${TOPOLOGY}"
  echo "  Topics   : ${MSG_TYPE}"
  echo "  Bag      : ${BAG_PATH}"
  echo "============================================="
  ```

- [ ] **Step 8: Update the post-startup print for MQTT per-robot**

  In the `mqtt)` case of the final print block, add per-robot variant. Replace:

  ```bash
      mqtt)
          echo "  Broker    : localhost:1883"
          echo "  Topics    : ros2/robot_<id>/gnss | /odom | /scan | /points"
          echo ""
          echo "  Quick check:"
          echo "    mosquitto_sub -h localhost -p 1883 -t 'ros2/#' -v"
          echo ""
          echo "  Consumer:"
          echo "    docker run --rm --network host ros2-fleet-consumer --broker mqtt"
          ;;
  ```

  With:

  ```bash
      mqtt)
          if [[ "${TOPOLOGY}" == "per-robot" ]]; then
              echo "  Brokers   : localhost:1883 … localhost:$((1882 + N)) (one per robot)"
              echo "  Topics    : ros2/robot_<id>/gnss | /odom | /scan | /points"
              echo ""
              echo "  Quick check (robot 1):"
              echo "    mosquitto_sub -h localhost -p 1883 -t 'ros2/#' -v"
              echo "  Quick check (robot 2):"
              echo "    mosquitto_sub -h localhost -p 1884 -t 'ros2/#' -v"
          else
              echo "  Broker    : localhost:1883"
              echo "  Topics    : ros2/robot_<id>/gnss | /odom | /scan | /points"
              echo ""
              echo "  Quick check:"
              echo "    mosquitto_sub -h localhost -p 1883 -t 'ros2/#' -v"
              echo ""
              echo "  Consumer:"
              echo "    docker run --rm --network host ros2-fleet-consumer --broker mqtt"
          fi
          ;;
  ```

- [ ] **Step 9: Smoke test — `--stop` with no running fleet exits cleanly**

  ```bash
  TOPOLOGY=per-robot N=3 BROKER=mqtt ./run.sh --stop
  echo "exit: $?"
  ```

  Expected: prints teardown message, exit code 0 (no crash even if nothing is running).

- [ ] **Step 10: Commit**

  ```bash
  git add run.sh
  git commit -m "feat(run): topology-aware broker startup and fleet file naming"
  ```

---

### Task 4: Update README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add `TOPOLOGY` to the optional env vars in the Quickstart section**

  In the `## Quickstart` section, after the existing examples, add:

  ```bash
  # 5 robots, each with its own Mosquitto broker (edge simulation)
  TOPOLOGY=per-robot N=5 BROKER=mqtt BAG_PATH=/tmp/my_bag_ros2 ./run.sh
  ```

- [ ] **Step 2: Add `TOPOLOGY` to the Payload format section's env var table**

  After the `## Payload format` section, add a new `## Topology` section:

  ```markdown
  ## Topology

  The `TOPOLOGY` environment variable selects how brokers are allocated:

  | Value | Description |
  |-------|-------------|
  | `shared` (default) | All robots write to one broker |
  | `per-robot` | Each robot gets its own broker (MQTT only) |

  ```bash
  # Shared broker (default)
  N=10 BROKER=mqtt BAG_PATH=/path/to/bag ./run.sh

  # Per-robot brokers: robot 1 → :1883, robot 2 → :1884, …
  TOPOLOGY=per-robot N=5 BROKER=mqtt BAG_PATH=/path/to/bag ./run.sh

  # Stop
  TOPOLOGY=per-robot N=5 BROKER=mqtt ./run.sh --stop
  ```

  In `per-robot` mode each robot's `header.frame_id` is set to `robot_<id>`
  so consumers can identify the source robot per message.
  ```

- [ ] **Step 3: Commit**

  ```bash
  git add README.md
  git commit -m "docs: document TOPOLOGY env var and per-robot broker mode"
  ```

---

### Task 5: Integration verification

**No new files — manual test against running containers.**

- [ ] **Step 1: Generate per-robot fleet file for N=2 and inspect it**

  ```bash
  TOPOLOGY=per-robot BROKER=mqtt N=2 bash gen_fleet.sh 2 /tmp/fleet_per_robot_2.yml
  cat /tmp/fleet_per_robot_2.yml
  ```

  Expected output contains:
  - `broker_1` with `- "1883:1883"`
  - `broker_2` with `- "1884:1883"`
  - `robot_1` with `BROKER_PORT: "1883"` and `depends_on: broker_1`
  - `robot_2` with `BROKER_PORT: "1884"` and `depends_on: broker_2`

- [ ] **Step 2: Validate compose file parses cleanly**

  ```bash
  docker compose -f /tmp/fleet_per_robot_2.yml config --quiet
  echo "exit: $?"
  ```

  Expected: no errors, exit code 0.

- [ ] **Step 3: Start per-robot fleet with N=2**

  ```bash
  TOPOLOGY=per-robot N=2 BROKER=mqtt BAG_PATH=/path/to/bag ./run.sh
  ```

  Expected banner:
  ```
  =============================================
    ROS 2 Robot Fleet
    Robots   : 2
    Broker   : mqtt
    Topology : per-robot
    Topics   : multi
    Bag      : /path/to/bag
  =============================================
  ```

- [ ] **Step 4: Verify both brokers are running**

  ```bash
  docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "broker|robot"
  ```

  Expected: `broker_1`, `broker_2`, `robot_1`, `robot_2` all `Up`.

- [ ] **Step 5: Check robot_1 logs for correct port and message_key**

  ```bash
  docker logs ros2-robot-fleet-demo-robot_1-1 2>&1 | grep -E "broker_port|message_key|ACTIVE|type support"
  ```

  Expected:
  ```
  mqtt.broker_port: 1883
  mqtt.message_key: "robot_1"
  Successfully loaded type support for JSON serialization of …
  /mosquitto_sink_1 ACTIVE.
  ```

- [ ] **Step 6: Subscribe to broker_1 and broker_2 separately and verify isolation**

  In two terminals (or background processes):

  ```bash
  # Terminal 1 — robot_1's broker
  timeout 5 docker run --rm --network host eclipse-mosquitto:2.0 \
    mosquitto_sub -h localhost -p 1883 -t 'ros2/#' -C 2 | python3 -c "
  import sys, json
  for line in sys.stdin:
      msg = json.loads(line)
      fid = msg.get('header', {}).get('frame_id', '?')
      print(f'broker_1 frame_id={fid}')
  "
  ```

  Expected: `frame_id=robot_1` in every message.

  ```bash
  # Terminal 2 — robot_2's broker
  timeout 5 docker run --rm --network host eclipse-mosquitto:2.0 \
    mosquitto_sub -h localhost -p 1884 -t 'ros2/#' -C 2 | python3 -c "
  import sys, json
  for line in sys.stdin:
      msg = json.loads(line)
      fid = msg.get('header', {}).get('frame_id', '?')
      print(f'broker_2 frame_id={fid}')
  "
  ```

  Expected: `frame_id=robot_2` in every message, and no messages from robot_1.

- [ ] **Step 7: Stop the fleet**

  ```bash
  TOPOLOGY=per-robot N=2 BROKER=mqtt ./run.sh --stop
  docker ps | grep -E "broker|robot" || echo "all stopped"
  ```

  Expected: `all stopped`.

- [ ] **Step 8: Verify shared topology still works (regression)**

  ```bash
  N=2 BROKER=mqtt BAG_PATH=/path/to/bag ./run.sh
  docker ps --format "{{.Names}}" | grep -E "broker|robot"
  # Should show: broker-1, robot_1-1, robot_2-1 (single shared broker)
  N=2 BROKER=mqtt ./run.sh --stop
  ```

  Expected: single `broker` service, not `broker_1`/`broker_2`.

- [ ] **Step 9: Final commit if any fixups needed, then push**

  ```bash
  git log --oneline origin/main..HEAD
  git push
  ```
