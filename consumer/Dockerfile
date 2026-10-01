ARG ROS_DISTRO=humble
FROM ros:${ROS_DISTRO}-ros-base

# Promote ARG to ENV so entrypoint.sh can source the correct ROS setup
ARG ROS_DISTRO=humble
ENV ROS_DISTRO=${ROS_DISTRO}

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-pip \
    ros-${ROS_DISTRO}-sensor-msgs \
    ros-${ROS_DISTRO}-nav-msgs \
 && rm -rf /var/lib/apt/lists/*

COPY consumer/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app
COPY consumer/consume.py /app/consume.py
COPY consumer/entrypoint.sh /app/entrypoint.sh
COPY examples/echo_gnss.py /app/echo_gnss.py

RUN chmod +x /app/entrypoint.sh /app/consume.py /app/echo_gnss.py

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["--broker", "kafka"]
