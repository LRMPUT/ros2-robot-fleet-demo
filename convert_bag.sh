#!/usr/bin/env bash
# Convert a ROS 1 (.bag) file to a ROS 2 bag directory readable by the fleet.
# Runs entirely in Docker — no local Python or ROS installation needed.
#
# Usage:
#   ./convert_bag.sh /path/to/recording.bag [output_dir]
#
# Output defaults to ./bags/<bag_stem>_ros2/
set -euo pipefail

SRC="$(realpath "${1:?Usage: ./convert_bag.sh <input.bag> [output_dir]}")"
STEM="$(basename "${SRC%.bag}")"
DST="$(realpath --canonicalize-missing "${2:-$(pwd)/bags/${STEM}_ros2}")"

if [[ ! -f "${SRC}" ]]; then
    echo "ERROR: ${SRC} not found." >&2
    exit 2
fi

if [[ -d "${DST}" ]]; then
    echo "Output directory already exists: ${DST}"
    echo "Delete it first if you want to reconvert."
    exit 0
fi

echo "============================================="
echo "  ROS 1 → ROS 2 bag conversion (Docker)"
echo "  Source : ${SRC}"
echo "  Output : ${DST}"
echo "============================================="

mkdir -p "${DST}"

docker run --rm \
    -v "${SRC}:/input/bag.bag:ro" \
    -v "${DST}:/output" \
    python:3.11-slim \
    bash -c "
        pip install --quiet rosbags && \
        rosbags-convert \
            --src /input/bag.bag \
            --dst /output \
            --dst-version 8 \
            --dst-typestore ros2_humble && \
        echo '' && \
        echo 'Topics found:' && \
        python3 -c \"
import rosbags.rosbag2 as rb2
with rb2.Reader('/output') as r:
    for conn in r.connections:
        print(f'  {conn.topic:<50} {conn.msgtype}')
\"
    "

echo ""
echo "Converted bag ready at: ${DST}"
echo ""
echo "Run the fleet:"
echo "  N=10 BROKER=kafka BAG_PATH=${DST} ./run.sh"
echo "  N=10 BROKER=mqtt  BAG_PATH=${DST} ./run.sh"
