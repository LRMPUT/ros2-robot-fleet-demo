package org.chistera;

import org.chistera.gnss.GNSS.MapGNSS;
import org.chistera.odom.ODOM.MapODOM;
import org.chistera.scan.SCAN.MapSCAN;
import org.chistera.points.POINTS.MapPOINTS;
import stream.nebula.operators.sinks.MQTTSink;
import stream.nebula.runtime.NebulaStreamRuntime;
import stream.nebula.runtime.Query;
import java.io.IOException;

public class CopyQueries {
    public static void main(String[] args) {
        try {
            String nesIp = System.getenv("NES_COORDINATOR_IP");
            String nesPortStr = System.getenv("NES_COORDINATOR_REST_PORT");
            String mqttIp = System.getenv("QUERY_HOST_IP");
            String mqttPortStr = System.getenv("QUERY_HOST_MQTT_PORT");

            int nesPort = Integer.parseInt(nesPortStr);
            String mqttUrl = mqttIp + ":" + mqttPortStr;

            NebulaStreamRuntime nebulaStreamRuntime = NebulaStreamRuntime.getRuntime(nesIp, nesPort);
            Query gnss_copy = nebulaStreamRuntime.readFromSource("gnss");
            Query odom_copy = nebulaStreamRuntime.readFromSource("odom");
            Query scan_copy = nebulaStreamRuntime.readFromSource("scan");
            Query points_copy = nebulaStreamRuntime.readFromSource("points");

            gnss_copy.map(new MapGNSS());
            odom_copy.map(new MapODOM());
            scan_copy.map(new MapSCAN());
            points_copy.map(new MapPOINTS());

            gnss_copy.sink(new MQTTSink(mqttUrl, "copy/gnss", "user", 1000, MQTTSink.TimeUnits.milliseconds, 0, MQTTSink.ServiceQualities.atLeastOnce, true));
            odom_copy.sink(new MQTTSink(mqttUrl, "copy/odom", "user", 1000, MQTTSink.TimeUnits.milliseconds, 0, MQTTSink.ServiceQualities.atLeastOnce, true));
            scan_copy.sink(new MQTTSink(mqttUrl, "copy/scan", "user", 1000, MQTTSink.TimeUnits.milliseconds, 0, MQTTSink.ServiceQualities.atLeastOnce, true));
            points_copy.sink(new MQTTSink(mqttUrl, "copy/points", "user", 1000, MQTTSink.TimeUnits.milliseconds, 0, MQTTSink.ServiceQualities.atLeastOnce, true));

            int gnss_query = nebulaStreamRuntime.executeQuery(gnss_copy, "BottomUp");
            System.out.println("gnss query started with ID: " + gnss_query);
            int odom_query = nebulaStreamRuntime.executeQuery(odom_copy, "BottomUp");
            System.out.println("odom query started with ID: " + odom_query);
            int scan_query = nebulaStreamRuntime.executeQuery(scan_copy, "BottomUp");
            System.out.println("scan query started with ID: " + scan_query);
            int points_query = nebulaStreamRuntime.executeQuery(points_copy, "BottomUp");
            System.out.println("points query started with ID: " + points_query);

        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(1);
        }
    }
}