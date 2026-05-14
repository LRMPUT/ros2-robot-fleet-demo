#!/usr/bin/env bash
# Example: simulate a multi-robot fleet using the INRAE Clermont-Ferrand
# leader/follower field campaign bag (CHIST-ERA project).
#
# The original ROS 1 bag contains one leader and one follower robot with:
#   NavSatFix   @ 10 Hz   (/follower/gps/fix, /leader/gps/fix)
#   Odometry    @ 20 Hz   (/follower/localisation/filtered_odom, ...)
#   LaserScan   @ 50 Hz   (/leader/lidar/scan)
#   PointCloud2 @ 12.5 Hz (/follower/lidar2/points)
#
# robot_replay.py selects topics by MESSAGE TYPE, not name, so it picks up
# the right topics automatically regardless of the bag's topic naming scheme.
# Each simulated robot gets its data shifted by Δlat/Δlon = 1e-4 × robot_id
# (~11 m grid) so the fleet appears spread across the field.
#
# Prerequisites:
#   1. Download the bag:
#        rorbots_follower_leader_parcelle_1MONT.bag
#      Available at: https://doi.org/10.5281/zenodo.XXXXXXX   [TODO: add DOI]
#
#   2. Convert to ROS 2:
#        ./convert_bag.sh rorbots_follower_leader_parcelle_1MONT.bag
#      → produces bags/rorbots_follower_leader_parcelle_1MONT_ros2/
#
# Usage:
#   ./examples/inrae_field_campaign.sh [kafka|mqtt] [num_robots]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${SCRIPT_DIR}"

BROKER="${1:-kafka}"
N="${2:-10}"

BAG_ROS2="${BAG_PATH:-bags/rorbots_follower_leader_parcelle_1MONT_ros2}"

if [[ ! -d "${BAG_ROS2}" ]]; then
    echo "Converted bag not found at: ${BAG_ROS2}"
    echo ""
    echo "Step 1 — download the ROS 1 bag:"
    echo "  rorbots_follower_leader_parcelle_1MONT.bag"
    echo "  (see README.md for the Zenodo DOI)"
    echo ""
    echo "Step 2 — convert:"
    echo "  ./convert_bag.sh rorbots_follower_leader_parcelle_1MONT.bag"
    echo ""
    echo "Or set BAG_PATH to an existing converted bag directory:"
    echo "  BAG_PATH=/path/to/bag_ros2 ./examples/inrae_field_campaign.sh"
    exit 1
fi

echo "============================================="
echo "  INRAE field campaign — ${N} robots"
echo "  Broker  : ${BROKER}"
echo "  Bag     : ${BAG_ROS2}"
echo "  Topics  : NavSatFix + Odometry + LaserScan + PointCloud2"
echo "============================================="
echo ""

N="${N}" BROKER="${BROKER}" BAG_PATH="${BAG_ROS2}" MSG_TYPE=multi ./run.sh

echo ""
echo "Live consumer (open a second terminal):"
echo "  cd consumer && python consume.py --broker ${BROKER} --stats-only"
