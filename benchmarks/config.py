"""Single source of truth for the geoflink geofence latency benchmark.

Mirrors the ksqlDB benchmark layout so the two stacks can be compared
apples-to-apples. Override the most common knobs from the CLI of each script
(see --help); everything else lives here.

Timestamp model recorded for every geofence alert:
    t0  event time   -- GPS header.stamp set by the dispatcher (ms)
    t1  ingest time  -- Kafka ROWTIME of the input ros2.fleet.gnss record,
                        i.e. when the broker received the message (ms)
    t2  geoflink time  -- wall clock when geoflink processed the row and is about
                        to write the alert to robot_geofence_alerts (ms)
    t3  arrival time -- wall clock when THIS consumer received the alert (ms)
"""

# ── Connectivity ──────────────────────────────────────────────────────────────
# Use localhost when running the harness on the host (WSL); use broker:29092
# and http://api:8000 when running inside the docker network (python-env).
API_URL = "http://localhost:8000"          # FastAPI REST layer (geoflink adapter)
KAFKA_BOOTSTRAP = "localhost:9092"         # broker EXTERNAL listener
ALERT_TOPIC = "output_geofence"      # geoflink geofence output topic

# ── Test sizing ─────────────────────────────────────────────────────────────
# Robots under test for a run: 1 .. 50. The 50-partition topic layout supports
# up to 50 robots without partition data skew. Override per run with --robots N.
NUM_ROBOTS = 10

# ── Zone under test ─────────────────────────────────────────────────────────
# 3MONT parcelle polygon (PostGIS EWKB HEX). The demo trajectories run OUTSIDE
# this polygon, so the geofence query fires an OUTSIDE alert for every robot on
# (almost) every GPS sample -> dense, deterministic latency samples.
# (1MONT was the opposite: robots sit inside it, so no OUTSIDE alerts fired.)
ZONE_ID = "3MONT"
ZONE_HEX = "0103000020e61000000100000005000000c04de780ff700b409cb09087b02b4740800b303ef36f0b40a86bdfc7ab2b4740802e6b6423700b40444bc4ae9d2b474000634496a4710b4070bf2fe2a62b4740c04de780ff700b409cb09087b02b4740"
CONFIG_NAME = "geofence"

# ── Run parameters ────────────────────────────────────────────────────────────
COLLECT_SECONDS = 60        # how long the collector records alerts
WARMUP_SECONDS = 5          # records in the first N seconds are dropped in stats
OUTPUT_DIR = "results_geoflink"      # written relative to this benchmarks/ dir


TEMPLATE_GEOFENCE = {
    "name": "geofence",
    "type": "GEOFENCE",
    "parallelism": 2,
    "bootStrapServers": "broker:29092",
    "localWebUi": False,
    "inputTopicName": "ros2.fleet.gnss",
    "range": 0.00000000001,
    "cellLengthMeters": 0,
    "uniformGridSize": 100,
    "gridMinX": 3.430,
    "gridMinY": 46.336,
    "gridMaxX": 3.436,
    "gridMaxY": 46.342,
}

def settle_seconds(n: int) -> int:
    """Settle wait before collecting, scaling with fleet size like run.sh's
    bringup_pad (15 + N/2): bigger fleets need longer to reach steady state."""
    return 15 + n // 2


def robot_ids(n: int) -> list[str]:
    """Return the robot ids for a run of size n (1..50)."""
    return [f"robot_{i}" for i in range(1, n + 1)]
