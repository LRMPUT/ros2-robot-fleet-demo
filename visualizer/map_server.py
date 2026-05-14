#!/usr/bin/env python3
"""Live map server for the ROS 2 robot fleet.

Subscribes to GNSS (NavSatFix) topics from Kafka or MQTT, then serves a
Leaflet.js map at http://localhost:8080 showing each robot's position
updating in real-time via Server-Sent Events.

Usage:
    python3 map_server.py --broker kafka
    python3 map_server.py --broker mqtt
    docker run --rm --network host -p 8080:8080 ros2-fleet-visualizer --broker kafka
"""
from __future__ import annotations

import argparse
import json as _json
import queue
import re
import threading
import time
from collections import defaultdict

from flask import Flask, Response

app = Flask(__name__)

# ── shared state ──────────────────────────────────────────────────────────────
_lock = threading.Lock()
_positions: dict[int, dict] = {}          # robot_id → {lat, lon, ts}
_msg_counts: dict[int, int] = defaultdict(int)
_subscribers: set[queue.Queue] = set()

COLORS = [
    "#e41a1c", "#377eb8", "#4daf4a", "#984ea3", "#ff7f00",
    "#a65628", "#f781bf", "#66c2a5", "#fc8d62", "#8da0cb",
    "#e78ac3", "#a6d854", "#ffd92f", "#e5c494", "#b3b3b3",
]


def _color(robot_id: int) -> str:
    return COLORS[(robot_id - 1) % len(COLORS)]


def _broadcast(event: dict) -> None:
    dead = set()
    for q in list(_subscribers):  # copy to avoid mutation-during-iteration
        try:
            q.put_nowait(event)
        except queue.Full:
            dead.add(q)
    for q in dead:
        _subscribers.discard(q)


def _on_gnss(robot_id: int, lat: float, lon: float) -> None:
    with _lock:
        _positions[robot_id] = {"lat": lat, "lon": lon, "ts": time.time()}
        _msg_counts[robot_id] += 1
    _broadcast({"type": "pos", "id": robot_id, "lat": lat, "lon": lon,
                "color": _color(robot_id)})


# ── payload decoding ──────────────────────────────────────────────────────────

def _from_json(payload: bytes) -> tuple[float, float] | None:
    try:
        d = _json.loads(payload)
        return float(d["latitude"]), float(d["longitude"])
    except Exception:
        return None


def _from_cdr(payload: bytes) -> tuple[float, float] | None:
    try:
        from rclpy.serialization import deserialize_message
        from sensor_msgs.msg import NavSatFix
        msg = deserialize_message(payload, NavSatFix)
        return msg.latitude, msg.longitude
    except Exception:
        return None


def _fmt_from_headers(headers) -> str | None:
    if not headers:
        return None
    for k, v in headers:
        if (k.decode() if isinstance(k, bytes) else k) == "payload_format":
            return v.decode() if isinstance(v, bytes) else v
    return None


def _decode(payload: bytes, fmt: str, headers=None) -> tuple[float, float] | None:
    if fmt == "auto":
        detected = _fmt_from_headers(headers)
        if detected:
            fmt = detected
        else:
            r = _from_json(payload)
            return r if r is not None else _from_cdr(payload)
    return _from_json(payload) if fmt == "json" else _from_cdr(payload)


# ── broker threads ────────────────────────────────────────────────────────────

def _kafka_thread(bootstrap: str, fmt: str) -> None:
    from confluent_kafka import Consumer, KafkaError
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"fleet-visualizer-{int(time.time())}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
        "topic.metadata.refresh.interval.ms": 500,
    })
    consumer.subscribe([r"^ros2\.robot_[0-9]+\.gnss$"])
    while True:
        msg = consumer.poll(timeout=0.3)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                print(f"[kafka] {msg.error()}")
            continue
        m = re.match(r"^ros2\.robot_(\d+)\.gnss$", msg.topic())
        if not m:
            continue
        pos = _decode(msg.value(), fmt, msg.headers())
        if pos:
            _on_gnss(int(m.group(1)), *pos)


def _mqtt_thread(host: str, port: int, fmt: str) -> None:
    import paho.mqtt.client as mqtt

    def on_message(_c, _u, msg):
        m = re.match(r"^ros2/robot_(\d+)/gnss$", msg.topic)
        if not m:
            return
        pos = _decode(msg.payload, fmt)
        if pos:
            _on_gnss(int(m.group(1)), *pos)

    client = mqtt.Client(f"fleet-visualizer-{int(time.time())}")
    client.on_message = on_message
    client.connect(host, port, keepalive=60)
    client.subscribe("ros2/+/gnss")
    client.loop_forever()


# ── HTTP routes ───────────────────────────────────────────────────────────────

_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Fleet Map</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
  body { margin: 0; font-family: monospace; background: #111; }
  #map { width: 100vw; height: 100vh; }
  #panel {
    position: absolute; top: 12px; right: 12px; z-index: 1000;
    background: rgba(0,0,0,0.78); color: #e0e0e0; padding: 14px 18px;
    border-radius: 8px; min-width: 220px; border: 1px solid #333;
  }
  #panel h3 { margin: 0 0 10px; font-size: 15px; color: #fff; }
  #panel .row { font-size: 12px; margin: 3px 0; }
  #panel .val { color: #7ecfff; }
  #robot-list { margin-top: 10px; max-height: 260px; overflow-y: auto; }
  .rrow { font-size: 11px; display: flex; align-items: center; gap: 6px; margin: 2px 0; }
  .dot { width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }
</style>
</head>
<body>
<div id="map"></div>
<div id="panel">
  <h3>&#x1F916; Fleet Monitor</h3>
  <div class="row">Robots: <span class="val" id="n-robots">0</span></div>
  <div class="row">GNSS rate: <span class="val" id="rate">0</span> msg/s</div>
  <div class="row">Broker: <span class="val">BROKER_PLACEHOLDER</span></div>
  <div id="robot-list"></div>
</div>
<script>
const map = L.map('map', {zoomControl: true});
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  attribution: '&copy; OpenStreetMap contributors', maxZoom: 20
}).addTo(map);
map.setView([0, 0], 2);

const markers = {};
const COLORS = {};
let totalMsgs = 0, lastMsgs = 0;
let fitted = false;

function dotIcon(color) {
  return L.divIcon({
    html: `<div style="background:${color};width:14px;height:14px;border-radius:50%;
            border:2px solid #fff;box-shadow:0 1px 4px rgba(0,0,0,.6)"></div>`,
    className: '', iconSize: [14,14], iconAnchor: [7,7], popupAnchor: [0,-12]
  });
}

function updateRobotList() {
  const ids = Object.keys(markers).map(Number).sort((a,b)=>a-b);
  document.getElementById('n-robots').textContent = ids.length;
  const el = document.getElementById('robot-list');
  el.innerHTML = ids.map(id =>
    `<div class="rrow"><div class="dot" style="background:${COLORS[id]}"></div>
     robot_${id}</div>`
  ).join('');
}

const es = new EventSource('/events');
es.onmessage = e => {
  const d = JSON.parse(e.data);
  if (d.type !== 'pos') return;
  const ll = [d.lat, d.lon];
  totalMsgs++;
  COLORS[d.id] = d.color;
  if (!markers[d.id]) {
    markers[d.id] = L.marker(ll, {icon: dotIcon(d.color)})
      .bindTooltip(`robot_${d.id}`, {permanent: true, direction: 'top', offset:[0,-10]})
      .addTo(map);
    if (!fitted && Object.keys(markers).length >= 2) {
      fitted = true;
      map.fitBounds(Object.values(markers).map(m => m.getLatLng()), {padding:[40,40]});
    } else if (Object.keys(markers).length === 1) {
      map.setView(ll, 17);
    }
    updateRobotList();
  } else {
    markers[d.id].setLatLng(ll);
  }
};
es.onerror = () => {
  document.getElementById('rate').textContent = 'reconnecting…';
};

setInterval(() => {
  const rate = totalMsgs - lastMsgs;
  lastMsgs = totalMsgs;
  document.getElementById('rate').textContent = rate;
}, 1000);
</script>
</body>
</html>
"""


@app.route("/")
def index():
    broker_label = app.config.get("BROKER_LABEL", "kafka")
    return _HTML.replace("BROKER_PLACEHOLDER", broker_label)


@app.route("/events")
def events():
    q: queue.Queue = queue.Queue(maxsize=500)
    with _lock:
        snapshot = dict(_positions)
    for robot_id, pos in snapshot.items():
        q.put_nowait({"type": "pos", "id": robot_id,
                      "lat": pos["lat"], "lon": pos["lon"],
                      "color": _color(robot_id)})
    _subscribers.add(q)

    def generate():
        try:
            while True:
                try:
                    data = q.get(timeout=15)
                    yield f"data: {_json.dumps(data)}\n\n"
                except queue.Empty:
                    yield 'data: {"type":"ping"}\n\n'
        finally:
            _subscribers.discard(q)

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ── entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Live map server for robot fleet")
    parser.add_argument("--broker", choices=["kafka", "mqtt"], default="kafka")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--mqtt-host", default="localhost")
    parser.add_argument("--mqtt-port", type=int, default=1883)
    parser.add_argument("--format", choices=["json", "cdr", "auto"], default="auto")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()

    if args.broker == "kafka":
        t = threading.Thread(target=_kafka_thread,
                             args=(args.bootstrap, args.format), daemon=True)
        label = f"kafka ({args.bootstrap})"
    else:
        t = threading.Thread(target=_mqtt_thread,
                             args=(args.mqtt_host, args.mqtt_port, args.format),
                             daemon=True)
        label = f"mqtt ({args.mqtt_host}:{args.mqtt_port})"
    t.start()

    app.config["BROKER_LABEL"] = label
    print(f"Map server ready → http://localhost:{args.port}   broker={label}")
    app.run(host="0.0.0.0", port=args.port, threaded=True)


if __name__ == "__main__":
    main()
