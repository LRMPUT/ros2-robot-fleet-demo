package org.chistera.scan;

import stream.nebula.udf.MapFunction;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SCAN {
    public static class SCANInput {
        public float angle_increment;
        public float angle_max;
        public float angle_min;
        public String header;
        public String intensities;
        public float range_max;
        public float range_min;
        public String ranges;
        public float scan_time;
        public float time_increment;
    }

    public static class SCANOutput {
        public float angle_increment;
        public float angle_max;
        public float angle_min;
        public String header_frame_id;
        public long header_stamp_nanosec;
        public int header_stamp_sec;
        public String intensities;
        public float range_max;
        public float range_min;
        public String ranges;
        public float scan_time;
        public float time_increment;
    }

    public static class MapSCAN implements MapFunction<SCANInput, SCANOutput> {
        private static final Pattern FRAME_ID_PATTERN = Pattern.compile("\"frame_id\":\"([^\"]+)\"");
        private static final Pattern NANOSEC_PATTERN = Pattern.compile("\"nanosec\":(-?\\d+)");
        private static final Pattern SEC_PATTERN = Pattern.compile("\"sec\":(-?\\d+)");

        @Override
        public SCANOutput map(final SCANInput input) {
            SCANOutput output = new SCANOutput();
            output.angle_increment = input.angle_increment;
            output.angle_max = input.angle_max;
            output.angle_min = input.angle_min;
            output.intensities = input.intensities;
            output.range_max = input.range_max;
            output.range_min = input.range_min;
            output.ranges = input.ranges;
            output.scan_time = input.scan_time;
            output.time_increment = input.time_increment;

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
            return output;
        }
    }
}