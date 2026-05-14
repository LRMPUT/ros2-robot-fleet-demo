ARG ROS_DISTRO=humble
FROM ros:${ROS_DISTRO}-ros-base

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-pip \
    ros-${ROS_DISTRO}-sensor-msgs \
    ros-${ROS_DISTRO}-nav-msgs \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir confluent-kafka paho-mqtt

WORKDIR /app
COPY consume.py /app/

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["bash", "-lc", "source /opt/ros/${ROS_DISTRO}/setup.bash && exec python3 /app/consume.py \"$@\"", "--"]
