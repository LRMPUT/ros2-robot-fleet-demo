#!/usr/bin/env bash
set -euo pipefail

N="${N:-10}"

# --- CONFIGURATION PARAMETERS ---
MQTT_BASE_PORT=1882
COORDINATOR_IP="127.0.0.1"
QUERY_HOST_IP="127.0.0.1"
COORDINATOR_RPC_PORT=4000
COORDINATOR_REST_PORT=8081
LOG_LEVEL="LOG_WARNING"

MOSQUITTO_IMAGE="eclipse-mosquitto"
NES_UI_IMAGE="nebulastream/nes-ui-image:latest"
NES_EXEC_IMAGE="nebulastream/nes-executable-image:latest"

DELAY_COORDINATOR=5
DELAY_WORKER=2

CONFIG_DIR="config"
COMPOSE_FILE="docker-compose.yml"
ENV_FILE=".env"
# --------------------------------

echo "Cleaning previous configuration..."

mkdir -p "${CONFIG_DIR}"
rm -f "${ENV_FILE}"
rm -f "${COMPOSE_FILE}"
rm -f "${CONFIG_DIR}/mosquitto.conf"
rm -f "${CONFIG_DIR}/nes_coordinator_config.yml"
rm -f "${CONFIG_DIR}/Dockerfile.copy"
rm -f "${CONFIG_DIR}/Dockerfile.geofence"
rm -f "${CONFIG_DIR}/Dockerfile.logger"
rm -f "${CONFIG_DIR}/nes_worker_"*.yml

echo "Creating core files..."

cat << EOF > "${ENV_FILE}"
NES_COORDINATOR_IP=${COORDINATOR_IP}
NES_COORDINATOR_RPC_PORT=${COORDINATOR_RPC_PORT}
NES_COORDINATOR_REST_PORT=${COORDINATOR_REST_PORT}
QUERY_HOST_IP=${QUERY_HOST_IP}
QUERY_HOST_MQTT_PORT=${MQTT_BASE_PORT}
EOF

cat << EOF > "${CONFIG_DIR}/mosquitto.conf"
listener ${MQTT_BASE_PORT}
allow_anonymous true
EOF

cat << 'EOF' > "${CONFIG_DIR}/nes_coordinator_config.yml"
logicalSources:
  - logicalSourceName: gnss
    fields:
      - name: altitude
        type: FLOAT64
      - name: header
        type: TEXT
      - name: latitude
        type: FLOAT64
      - name: longitude
        type: FLOAT64
      - name: position_covariance
        type: TEXT
      - name: position_covariance_type
        type: INT16
      - name: status
        type: TEXT
  - logicalSourceName: odom
    fields:
      - name: child_frame_id
        type: TEXT
      - name: header
        type: TEXT
      - name: pose
        type: TEXT
      - name: twist
        type: TEXT
  - logicalSourceName: scan
    fields:
      - name: angle_increment
        type: FLOAT32
      - name: angle_max
        type: FLOAT32
      - name: angle_min
        type: FLOAT32
      - name: header
        type: TEXT
      - name: intensities
        type: TEXT
      - name: range_max
        type: FLOAT32
      - name: range_min
        type: FLOAT32
      - name: ranges
        type: TEXT
      - name: scan_time
        type: FLOAT32
      - name: time_increment
        type: FLOAT32
  - logicalSourceName: points
    fields:
      - name: data
        type: TEXT
      - name: fields
        type: TEXT
      - name: header
        type: TEXT
      - name: height
        type: INT64
      - name: is_bigendian
        type: BOOLEAN
      - name: is_dense
        type: BOOLEAN
      - name: point_step
        type: INT64
      - name: row_step
        type: INT64
      - name: width
        type: INT64
EOF

cat << 'EOF' > "${CONFIG_DIR}/Dockerfile.copy"
FROM gradle:8.5-jdk17 AS builder
WORKDIR /app
COPY query/CopyQueries/settings.gradle.kts query/CopyQueries/build.gradle.kts ./
RUN gradle shadowJar --no-daemon || true
COPY query/CopyQueries/src ./src
RUN gradle shadowJar --no-daemon

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=builder /app/build/libs/*-all.jar /app/queries.jar
ENTRYPOINT ["java", "-jar", "/app/queries.jar"]
EOF

cat << 'EOF' > "${CONFIG_DIR}/Dockerfile.geofence"
FROM gradle:8.5-jdk17 AS builder
WORKDIR /app
COPY query/GeofenceQuery/settings.gradle.kts query/GeofenceQuery/build.gradle.kts ./
RUN gradle shadowJar --no-daemon || true
COPY query/GeofenceQuery/src ./src
RUN gradle shadowJar --no-daemon

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=builder /app/build/libs/*-all.jar /app/geofence.jar
COPY config/1_MONT.txt /app/1_MONT.txt
ENTRYPOINT ["java", "-jar", "/app/geofence.jar", "/app/1_MONT.txt"]
EOF

cat << 'EOF' > "${CONFIG_DIR}/Dockerfile.logger"
FROM python:3-slim
RUN pip install --upgrade pip
RUN pip install paho-mqtt
EOF

cat << EOF > "${COMPOSE_FILE}"
services:

  nes_mqtt:
    image: ${MOSQUITTO_IMAGE}
    network_mode: host
    volumes:
      - ./${CONFIG_DIR}:/mosquitto/config

  nes_ui:
    image: ${NES_UI_IMAGE}
    network_mode: host
    env_file: ${ENV_FILE}

  copy_queries:
    build:
      context: .
      dockerfile: ${CONFIG_DIR}/Dockerfile.copy
    network_mode: host
    env_file: ${ENV_FILE}

  geofence_query:
    build:
      context: .
      dockerfile: ${CONFIG_DIR}/Dockerfile.geofence
    network_mode: host
    env_file: ${ENV_FILE}

  nes_coordinator:
    image: ${NES_EXEC_IMAGE}
    network_mode: host
    volumes:
      - ./${CONFIG_DIR}:/config
    command:
      - nesCoordinator
      - --coordinatorHost=${COORDINATOR_IP}
      - --restIp=${COORDINATOR_IP}
      - --restPort=${COORDINATOR_REST_PORT}
      - --rpcPort=${COORDINATOR_RPC_PORT}
      - --worker.coordinatorHost=${COORDINATOR_IP}
      - --worker.coordinatorPort=${COORDINATOR_RPC_PORT}
      - --worker.localWorkerHost=${COORDINATOR_IP}
      - --logLevel=${LOG_LEVEL}
      - --configPath=/config/nes_coordinator_config.yml
    env_file: ${ENV_FILE}
  
  python_logger:
    build:
      context: .
      dockerfile: ${CONFIG_DIR}/Dockerfile.logger
    network_mode: host
    volumes:
      - ./scripts:/scripts
      - ./logs:/logs
    command: [ "python", "/scripts/logger.py", "${QUERY_HOST_IP}", "${MQTT_BASE_PORT}" ]
    env_file: ${ENV_FILE}
EOF

echo "Generating worker configurations and appending to docker-compose.yml..."

for ((i=1; i<=N; i++)); do
    WORKER_ID=$((i + 1))
    WORKER_PORT=$((MQTT_BASE_PORT + i))
    
    cat << EOF > "${CONFIG_DIR}/nes_worker_${i}_config.yml"
coordinatorHost: ${COORDINATOR_IP}
coordinatorPort: ${COORDINATOR_RPC_PORT}
localWorkerHost: ${COORDINATOR_IP}
workerId: ${WORKER_ID}
logLevel: ${LOG_LEVEL}

physicalSources:
  - logicalSourceName: gnss
    physicalSourceName: robot_${i}_gnss
    type: MQTT_SOURCE
    configuration:
      url: ${COORDINATOR_IP}:${WORKER_PORT}
      topic: ros2/robot_${i}/gnss
  - logicalSourceName: odom
    physicalSourceName: robot_${i}_odom
    type: MQTT_SOURCE
    configuration:
      url: ${COORDINATOR_IP}:${WORKER_PORT}
      topic: ros2/robot_${i}/odom
  - logicalSourceName: scan
    physicalSourceName: robot_${i}_scan
    type: MQTT_SOURCE
    configuration:
      url: ${COORDINATOR_IP}:${WORKER_PORT}
      topic: ros2/robot_${i}/scan
  - logicalSourceName: points
    physicalSourceName: robot_${i}_points
    type: MQTT_SOURCE
    configuration:
      url: ${COORDINATOR_IP}:${WORKER_PORT}
      topic: ros2/robot_${i}/points
EOF

    cat << EOF >> "${COMPOSE_FILE}"

  nes_worker_${i}:
    image: ${NES_EXEC_IMAGE}
    network_mode: host
    volumes:
      - ./${CONFIG_DIR}:/config
    command:
      - nesWorker
      - --configPath=/config/nes_worker_${i}_config.yml
    env_file: ${ENV_FILE}
EOF
done

echo "Running NebulaStream..."

docker compose up nes_mqtt nes_ui nes_coordinator -d
sleep "${DELAY_COORDINATOR}"

for ((i=1; i<=N; i++)); do
    sleep "${DELAY_WORKER}"
    docker compose up nes_worker_${i} -d
done

echo "NebulaStream works"

echo "Submitting queries..."

# docker compose up --build copy_queries -d
docker compose up --build geofence_query -d

docker compose up --build python_logger -d