#!/bin/bash
set -euo pipefail
source /opt/ros/${ROS_DISTRO}/setup.bash

# If first arg is a python3 call or a .py file, exec directly.
# Otherwise pass all args to consume.py (default behaviour).
if [[ "${1:-}" == "python3" ]] || [[ "${1:-}" == *.py ]]; then
    exec "$@"
else
    exec python3 /app/consume.py "$@"
fi
