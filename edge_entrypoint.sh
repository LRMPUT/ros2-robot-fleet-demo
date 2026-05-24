#!/usr/bin/env bash
# Edge topology entrypoint: one container runs BOTH the robot publisher AND
# its own kafka_sink (or mosquitto_sink), so each robot has a private
# Dispatcher instance that talks DDS locally and produces directly to the broker.
#
# Fleet routing (FLEET_ROUTING=1, default for Kafka):
#   All robots write to ONE shared Kafka topic (ros2.fleet.<suffix>) using
#   kafka_key=robot_<id> to distinguish robots.  This allows a single ksqlDB
#   query to process data from all robots.
#
#   ros2.fleet.gnss  key=robot_1  ← robot 1
#   ros2.fleet.gnss  key=robot_2  ← robot 2   → ksqlDB fleet_gnss stream
#   ros2.fleet.gnss  key=robot_N  ← robot N
#
# Per-robot routing (FLEET_ROUTING=0):
#   Each robot writes to its own topic (ros2.robot_<id>.<suffix>).
#   Useful for debugging individual robots or non-ksqlDB consumers.
set -euo pipefail

: "${ROBOT_ID:?ROBOT_ID env var is required}"
: "${SINK_KIND:=kafka}"          # 'kafka' or 'mqtt'
: "${BAG_PATH:?BAG_PATH env var is required}"
: "${BROKER_HOST:=localhost}"
: "${MSG_TYPE:=multi}"
: "${RATE_HZ:=10}"
: "${MQTT_QOS:=1}"               # 0 = fire-and-forget, 1 = at-least-once
# Fleet routing: 1 (default for Kafka, enables single ksqlDB topic),
#                0 to use per-robot topics.
# MQTT always uses per-robot topics (fleet routing not yet supported).
if [[ "${SINK_KIND}" == "mqtt" ]]; then
    : "${FLEET_ROUTING:=0}"
else
    : "${FLEET_ROUTING:=1}"
fi
# JSON default for Kafka; CDR default for MQTT (mosquitto_sink JSON path has
# introspection issues with nested message types such as NavSatFix).
if [[ "${SINK_KIND}" == "mqtt" ]]; then
    : "${PAYLOAD_FORMAT:=cdr}"
else
    : "${PAYLOAD_FORMAT:=json}"
fi

# Isolate each robot's DDS traffic in its own domain so the lifecycle
# discovery doesn't compete across containers on the shared host network.
# Domain IDs 0–101 are valid; robot IDs above 101 wrap around.
#export ROS_DOMAIN_ID=$(( (ROBOT_ID - 1) % 101 + 1 ))
NUMERIC_ID="${ROBOT_ID%%_*}"
export ROS_DOMAIN_ID=$(( (NUMERIC_ID - 1) % 101 + 1 ))

# ROS setup scripts reference unset variables; relax set -u for the source step.
set +u
source /opt/ros/humble/setup.bash
source /ws/install/setup.bash
set -u

# Build the per-robot subscription list (4 entries in multi mode).
case "${MSG_TYPE}" in
    navsatfix)   PAIRS=("sensor_msgs/msg/NavSatFix|gnss") ;;
    odometry)    PAIRS=("nav_msgs/msg/Odometry|odom") ;;
    laserscan)   PAIRS=("sensor_msgs/msg/LaserScan|scan") ;;
    pointcloud2) PAIRS=("sensor_msgs/msg/PointCloud2|points") ;;
    multi)
        PAIRS=("sensor_msgs/msg/NavSatFix|gnss"
               "nav_msgs/msg/Odometry|odom"
               "sensor_msgs/msg/LaserScan|scan"
               "sensor_msgs/msg/PointCloud2|points")
        ;;
    *) echo "Unknown MSG_TYPE=${MSG_TYPE}" >&2; exit 1 ;;
esac

# Build subscriptions_yaml.
# Fleet routing: all robots share one Kafka topic, distinguished by kafka_key.
# Per-robot routing: each robot gets its own topic (ros2.robot_<id>.<suffix>).
SUBS_YAML=""
for p in "${PAIRS[@]}"; do
    IFS='|' read -r ros_type suffix <<< "$p"
    if [[ "${FLEET_ROUTING}" == "1" && "${SINK_KIND}" == "kafka" ]]; then
        SUBS_YAML+="- topic_name: /robot_${ROBOT_ID}/${suffix}
  msg_type: ${ros_type}
  kafka_name: fleet.${suffix}
  kafka_key: robot_${ROBOT_ID}
"
    else
        SUBS_YAML+="- topic_name: /robot_${ROBOT_ID}/${suffix}
  msg_type: ${ros_type}
"
    fi
done

NODE_NAME="kafka_sink_${ROBOT_ID}"
EXE="kafka_sink_node_exe"
PKG="kafka_sink"
case "${SINK_KIND}" in
    kafka)
        PARAMS_FILE="/tmp/sink_params_${ROBOT_ID}.yaml"
        cat > "${PARAMS_FILE}" <<EOF
${NODE_NAME}:
  ros__parameters:
    subscriptions_yaml: |
$(printf '%s\n' "${SUBS_YAML}" | sed 's/^/      /')
    kafka.payload_format: "${PAYLOAD_FORMAT}"
    kafka.bootstrap_servers: "${BROKER_HOST}:9092"
    kafka.topic_prefix: ros2
    kafka.message_key: "robot_${ROBOT_ID}"
    kafka.drop_when_full: true
    kafka.strict_startup: false
    kafka.acks: "1"
    kafka.linger_ms: 0
    metrics.enabled: false
EOF
        ;;
    mqtt)
        EXE="mosquitto_sink_node_exe"
        PKG="mosquitto_sink"
        NODE_NAME="mosquitto_sink_${ROBOT_ID}"
        PARAMS_FILE="/tmp/sink_params_${ROBOT_ID}.yaml"
        cat > "${PARAMS_FILE}" <<EOF
${NODE_NAME}:
  ros__parameters:
    subscriptions_yaml: |
$(printf '%s\n' "${SUBS_YAML}" | sed 's/^/      /')
    mqtt.broker_host: "${BROKER_HOST}"
    mqtt.broker_port: 1883
    mqtt.client_id: "mosquitto_sink_${ROBOT_ID}"
    mqtt.topic_prefix: ros2
    mqtt.qos: ${MQTT_QOS}
    mqtt.payload_format: "${PAYLOAD_FORMAT}"
    metrics.enabled: false
EOF
        ;;
    *) echo "Unknown SINK_KIND=${SINK_KIND}" >&2; exit 1 ;;
esac

echo "[edge] robot_id=${ROBOT_ID} sink=${SINK_KIND} fleet_routing=${FLEET_ROUTING} payload=${PAYLOAD_FORMAT}"
echo "[edge] params:"; sed 's/^/  | /' "${PARAMS_FILE}"

# 1. Start the per-robot sink in the background with a unique node name.
ros2 run "${PKG}" "${EXE}" --ros-args \
    -r __node:="${NODE_NAME}" \
    --params-file "${PARAMS_FILE}" &
SINK_PID=$!

cleanup() {
    kill "${SINK_PID}" 2>/dev/null || true
}
trap cleanup EXIT

# 2. Wait for the sink lifecycle service, then configure + activate with retries.
echo "[edge] waiting for /${NODE_NAME} lifecycle to stabilize..."
for i in {1..15}; do
    if ros2 lifecycle set "${NODE_NAME}" configure >/dev/null 2>&1; then
        echo "[edge] /${NODE_NAME} configured successfully."
        break
    fi
    echo "    [edge] Node discovery lagging under high CPU load, retrying in 1s... ($i/15)"
    sleep 1
done

ros2 lifecycle set "${NODE_NAME}" activate
echo "[edge] /${NODE_NAME} ACTIVE."

# 3. Run the publisher in the foreground. When it exits, cleanup() kills the sink.
exec python3 /app/robot_replay.py
