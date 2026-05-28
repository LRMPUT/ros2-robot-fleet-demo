package org.chistera;

import org.chistera.geofence.Geofence.MapGeofence;
import org.chistera.timestamp.Timestamp.MapTimestamp;
import stream.nebula.operators.sinks.MQTTSink;
import stream.nebula.runtime.NebulaStreamRuntime;
import stream.nebula.runtime.Query;
import static stream.nebula.expression.Expressions.attribute;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class GeofenceQuery {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: GeofenceQuery <wkb_file_path>");
            System.exit(1);
        }

        try {
            String wkbHex = new String(Files.readAllBytes(Paths.get(args[0]))).trim();

            String nesIp = System.getenv("NES_COORDINATOR_IP");
            String nesPortStr = System.getenv("NES_COORDINATOR_REST_PORT");
            String mqttIp = System.getenv("QUERY_HOST_IP");
            String mqttPortStr = System.getenv("QUERY_HOST_MQTT_PORT");

            int nesPort = Integer.parseInt(nesPortStr);
            String mqttUrl = mqttIp + ":" + mqttPortStr;

            NebulaStreamRuntime nebulaStreamRuntime = NebulaStreamRuntime.getRuntime(nesIp, nesPort);
            Query geofence = nebulaStreamRuntime.readFromSource("gnss");

            geofence.map(new MapGeofence(wkbHex));
            geofence.filter(attribute("exited"));
            geofence.map(new MapTimestamp());

            geofence.sink(new MQTTSink(mqttUrl, "geofence", "user", 1000, MQTTSink.TimeUnits.milliseconds, 0, MQTTSink.ServiceQualities.atLeastOnce, true));
            
            int geofence_query = nebulaStreamRuntime.executeQuery(geofence, "BottomUp");
            System.out.println("Geofence query started with ID: " + geofence_query);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(1);
        }
    }
}