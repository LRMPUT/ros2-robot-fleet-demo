# Pipeline from a ROS2 message to the result of a NebulaStream query

1. A ROS2 message is captured by a ROS2 dispatcher, serialized to JSON, and published to an MQTT topic on a Mosquitto broker.

2. The address of the broker is specified as a physical source for a NebulaStream worker.

3. The worker receives a partial query plan and preprocesses the data before sending it to the coordinator.

4. The coordinator gathers all data from all workers and produces the final results, which are sent to MQTT result topics and can be received by the following script:

``` python
import paho.mqtt.client as mqtt

BROKER_IP = "127.0.0.1"
PORT = 1882
TOPIC = "geofence"

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.subscribe(TOPIC)
    else:
        print(f"Connection failed with code: {rc}")

def on_message(client, userdata, msg):
    try:
        payload_str = msg.payload.decode()
        output_string = f"[{msg.topic}] {payload_str}"
        print(output_string, flush=True)
    except Exception as e:
        print(f"Error processing message: {e}", flush=True)

client = mqtt.Client(client_id="geofence_listener")

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
```

## geofencing_query

### JSON Structure
| Field | Type | Description |
| :--- | :--- | :--- |
| `gnss$altitude` | double | Altitude in meters above the WGS 84 ellipsoid |
| `gnss$exited` | boolean | Indicates whether the robot has exited the specified area |
| `gnss$header_frame_id` | String | Robot identifier |
| `gnss$header_stamp_nanosec` | long | Nanoseconds component of the message timestamp |
| `gnss$header_stamp_sec` | int | Seconds component of the message timestamp |
| `gnss$latitude` | double | Latitude in degrees |
| `gnss$longitude` | double | Longitude in degrees |
| `gnss$position_covariance` | String | Pose covariance matrix (in ENU), stored as a string of double array |
| `gnss$position_covariance_type` | short | Method of covariance estimation |
| `gnss$processing_timestamp` | long | Processing end timestamp |
| `gnss$status_service` | int | Supported satellite services |
| `gnss$status_status` | byte | Satellite fix status |

**Example:**
```json
{
    "gnss$altitude": 366.80832296796143,
    "gnss$exited": true,
    "gnss$header_frame_id": "robot_6",
    "gnss$header_stamp_nanosec": 692342774,
    "gnss$header_stamp_sec": 1779987053,
    "gnss$latitude": 46.33911092646544,
    "gnss$longitude": 3.4338277129460884,
    "gnss$position_covariance": "[0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0]",
    "gnss$position_covariance_type": 0,
    "gnss$processing_timestamp": 1779987053693498723,
    "gnss$status_service": 1,
    "gnss$status_status": 2
}
```
