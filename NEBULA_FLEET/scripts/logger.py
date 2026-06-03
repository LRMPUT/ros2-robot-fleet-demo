import paho.mqtt.client as mqtt
import time
import json
import sys
import os

if len(sys.argv) < 3:
    print("Usage: python script.py <BROKER_IP> <PORT>")
    sys.exit(1)

BROKER_IP = sys.argv[1]
PORT = int(sys.argv[2])
TOPIC = "geofence"
LOG_DIR = "/logs"
LOG_FILE = os.path.join(LOG_DIR, "timestamps.txt")

os.makedirs(LOG_DIR, exist_ok=True)

if not os.path.exists(LOG_FILE) or os.path.getsize(LOG_FILE) == 0:
    with open(LOG_FILE, "w") as file:
        file.write("robot_id\tt0_ns\tt1_ns\tt2_ns\tt3_ns\n")

def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        client.subscribe(TOPIC)
        print("Connected and subscribed.", flush=True)
    else:
        print(f"Connection failed with code: {reason_code}", flush=True)

def on_message(client, userdata, msg):
    arrival_ts = time.time_ns()
    try:
        payload_str = msg.payload.decode()
        data = json.loads(payload_str)
        
        robot_id = data.get("gnss$header_frame_id", "")
        msg_ts_sec = data.get("gnss$t0_ns", "").strip('"')
        msg_ts_ns = data.get("gnss$t1_ns", "").strip('"')
        proc_ts = data.get("gnss$t2_ns", "").strip('"')
        
        log_line = f"{robot_id}\t{msg_ts_sec}\t{msg_ts_ns}\t{proc_ts}\t{arrival_ts}\n"
        
        with open(LOG_FILE, "a") as file:
            file.write(log_line)
            
        print(f"Logged payload from: {robot_id}", flush=True)
    except json.JSONDecodeError as e:
        print(f"JSON decoding error: {e}", flush=True)
    except Exception as e:
        print(f"Error processing message: {e}", flush=True)

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="geofence_listener")

client.on_connect = on_connect
client.on_message = on_message

try:
    client.connect(BROKER_IP, PORT, 60)
    client.loop_forever()
except KeyboardInterrupt:
    print("Stopping listener...")
except Exception as e:
    print(f"Connection error: {e}")
finally:
    client.disconnect()