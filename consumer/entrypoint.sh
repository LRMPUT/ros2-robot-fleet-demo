#!/bin/bash
set -eo pipefail
# ROS setup.bash references unbound vars internally — disable -u around it
set +u
source /opt/ros/${ROS_DISTRO}/setup.bash
set -u

# If first arg is a python3 call or a .py file, exec directly.
# Otherwise pass all args to consume.py (default behaviour).
if [[ "${1:-}" == "python3" ]] || [[ "${1:-}" == *.py ]]; then
    exec "$@"
else
    exec python3 /app/consume.py "$@"
fi
