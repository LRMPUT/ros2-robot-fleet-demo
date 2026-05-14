ARG ROS_DISTRO=humble
FROM ros:${ROS_DISTRO}-ros-base

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-pip \
    ros-${ROS_DISTRO}-sensor-msgs \
    ros-${ROS_DISTRO}-nav-msgs \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir confluent-kafka paho-mqtt

WORKDIR /app
COPY consumer/consume.py /app/consume.py
COPY consumer/entrypoint.sh /app/entrypoint.sh
COPY examples/echo_gnss.py /app/echo_gnss.py

RUN chmod +x /app/entrypoint.sh

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["/app/entrypoint.sh"]
