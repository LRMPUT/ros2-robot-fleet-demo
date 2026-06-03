package org.chistera.geofence;

import stream.nebula.udf.MapFunction;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Geofence {
    public static class GeofenceInput {
        public double altitude;
        public String header;
        public double latitude;
        public double longitude;
        public String position_covariance;
        public short position_covariance_type;
        public String status;
        public String _ts;
    }

    public static class GeofenceOutput {
        public double altitude;
        public String header_frame_id;
        public String header_stamp_nanosec;
        public String header_stamp_sec;
        public double latitude;
        public double longitude;
        public String position_covariance;
        public short position_covariance_type;
        public int status_service;
        public byte status_status;
        public String t0_ns;
        public String t1_ns;
        public boolean exited;
    }

    public static class MapGeofence implements MapFunction<GeofenceInput, GeofenceOutput> {
        private static final Pattern FRAME_ID_PATTERN = Pattern.compile("\"frame_id\":\"([^\"]+)\"");
        private static final Pattern NANOSEC_PATTERN = Pattern.compile("\"nanosec\":(-?\\d+)");
        private static final Pattern SEC_PATTERN = Pattern.compile("\"sec\":(-?\\d+)");
        private static final Pattern SERVICE_PATTERN = Pattern.compile("\"service\":(-?\\d+)");
        private static final Pattern STATUS_PATTERN = Pattern.compile("\"status\":(-?\\d+)");
        private static final Pattern T0_NS_PATTERN = Pattern.compile("\"t0_ns\":(-?\\d+)");
        private static final Pattern T1_NS_PATTERN = Pattern.compile("\"t1_ns\":(-?\\d+)");

        private String wkbHex;
        private transient Geometry polygon;
        private transient GeometryFactory geometryFactory;

        public MapGeofence() {}

        public MapGeofence(String wkbHex) {
            this.wkbHex = wkbHex;
        }

        private void initGeometry() {
            if (this.geometryFactory == null) {
                this.geometryFactory = new GeometryFactory();
            }
            if (this.polygon == null && this.wkbHex != null && !this.wkbHex.isEmpty()) {
                try {
                    byte[] wkbBytes = hexStringToByteArray(this.wkbHex);
                    WKBReader reader = new WKBReader(this.geometryFactory);
                    this.polygon = reader.read(wkbBytes);
                } catch (ParseException e) {
                    throw new RuntimeException("Failed to parse WKB geometry", e);
                }
            }
        }

        private byte[] hexStringToByteArray(String s) {
            int len = s.length();
            byte[] data = new byte[len / 2];
            for (int i = 0; i < len; i += 2) {
                data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                                     + Character.digit(s.charAt(i+1), 16));
            }
            return data;
        }

        @Override
        public GeofenceOutput map(final GeofenceInput input) {
            initGeometry();

            GeofenceOutput output = new GeofenceOutput();
            output.altitude = input.altitude;
            output.latitude = input.latitude;
            output.longitude = input.longitude;
            output.position_covariance = input.position_covariance;
            output.position_covariance_type = input.position_covariance_type;
            output.header_frame_id = null;
            output.header_stamp_nanosec = "0";
            output.header_stamp_sec = "0";

            if (input.header != null) {
                Matcher frameMatcher = FRAME_ID_PATTERN.matcher(input.header);
                if (frameMatcher.find()) {
                    output.header_frame_id = frameMatcher.group(1);
                }
                Matcher nanoMatcher = NANOSEC_PATTERN.matcher(input.header);
                if (nanoMatcher.find()) {
                    output.header_stamp_nanosec = nanoMatcher.group(1);
                }
                Matcher secMatcher = SEC_PATTERN.matcher(input.header);
                if (secMatcher.find()) {
                    output.header_stamp_sec = secMatcher.group(1);
                }
            }

            output.status_service = 0;
            output.status_status = 0;

            if (input.status != null) {
                Matcher serviceMatcher = SERVICE_PATTERN.matcher(input.status);
                if (serviceMatcher.find()) {
                    output.status_service = Integer.parseInt(serviceMatcher.group(1));
                }
                Matcher statusMatcher = STATUS_PATTERN.matcher(input.status);
                if (statusMatcher.find()) {
                    output.status_status = Byte.parseByte(statusMatcher.group(1));
                }
            }

            output.t0_ns = "0";
            output.t1_ns = "0";

            if (input._ts != null) {
                Matcher t0Matcher = T0_NS_PATTERN.matcher(input._ts);
                if (t0Matcher.find()) {
                    output.t0_ns = t0Matcher.group(1);
                }
                
                Matcher t1Matcher = T1_NS_PATTERN.matcher(input._ts);
                if (t1Matcher.find()) {
                    output.t1_ns = t1Matcher.group(1);
                }
            }

            if (this.polygon != null) {
                Point point = this.geometryFactory.createPoint(new Coordinate(input.longitude, input.latitude));
                output.exited = !this.polygon.contains(point);
            } else {
                output.exited = false;
            }

            return output;
        }
    }
}