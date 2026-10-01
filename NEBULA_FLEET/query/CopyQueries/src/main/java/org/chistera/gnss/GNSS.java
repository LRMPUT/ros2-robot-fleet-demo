package org.chistera.gnss;

import stream.nebula.udf.MapFunction;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GNSS {
    public static class GNSSInput {
        public double altitude;
        public String header;
        public double latitude;
        public double longitude;
        public String position_covariance;
        public short position_covariance_type;
        public String status;
    }

    public static class GNSSOutput {
        public double altitude;
        public String header_frame_id;
        public long header_stamp_nanosec;
        public int header_stamp_sec;
        public double latitude;
        public double longitude;
        public String position_covariance;
        public short position_covariance_type;
        public int status_service;
        public byte status_status;
    }

    public static class MapGNSS implements MapFunction<GNSSInput, GNSSOutput> {
        private static final Pattern FRAME_ID_PATTERN = Pattern.compile("\"frame_id\":\"([^\"]+)\"");
        private static final Pattern NANOSEC_PATTERN = Pattern.compile("\"nanosec\":(-?\\d+)");
        private static final Pattern SEC_PATTERN = Pattern.compile("\"sec\":(-?\\d+)");
        private static final Pattern SERVICE_PATTERN = Pattern.compile("\"service\":(-?\\d+)");
        private static final Pattern STATUS_PATTERN = Pattern.compile("\"status\":(-?\\d+)");

        @Override
        public GNSSOutput map(final GNSSInput input) {
            GNSSOutput output = new GNSSOutput();
            output.altitude = input.altitude;
            output.latitude = input.latitude;
            output.longitude = input.longitude;
            output.position_covariance = input.position_covariance;
            output.position_covariance_type = input.position_covariance_type;
            output.header_frame_id = null;
            output.header_stamp_nanosec = 0L;
            output.header_stamp_sec = 0;

            if (input.header != null) {
                Matcher frameMatcher = FRAME_ID_PATTERN.matcher(input.header);
                if (frameMatcher.find()) {
                    output.header_frame_id = frameMatcher.group(1);
                }
                Matcher nanoMatcher = NANOSEC_PATTERN.matcher(input.header);
                if (nanoMatcher.find()) {
                    output.header_stamp_nanosec = Long.parseLong(nanoMatcher.group(1));
                }
                Matcher secMatcher = SEC_PATTERN.matcher(input.header);
                if (secMatcher.find()) {
                    output.header_stamp_sec = Integer.parseInt(secMatcher.group(1));
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
            return output;
        }
    }
}