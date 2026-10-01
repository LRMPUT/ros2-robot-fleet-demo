package org.chistera.points;

import stream.nebula.udf.MapFunction;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class POINTS {
    public static class POINTSInput {
        public String data;
        public String fields;
        public String header;
        public long height;
        public boolean is_bigendian;
        public boolean is_dense;
        public long point_step;
        public long row_step;
        public long width;
    }

    public static class POINTSOutput {
        public String data;
        public String fields;
        public String header_frame_id;
        public long header_stamp_nanosec;
        public int header_stamp_sec;
        public long height;
        public boolean is_bigendian;
        public boolean is_dense;
        public long point_step;
        public long row_step;
        public long width;
    }

    public static class MapPOINTS implements MapFunction<POINTSInput, POINTSOutput> {
        private static final Pattern FRAME_ID_PATTERN = Pattern.compile("\"frame_id\":\"([^\"]+)\"");
        private static final Pattern NANOSEC_PATTERN = Pattern.compile("\"nanosec\":(-?\\d+)");
        private static final Pattern SEC_PATTERN = Pattern.compile("\"sec\":(-?\\d+)");

        @Override
        public POINTSOutput map(final POINTSInput input) {
            POINTSOutput output = new POINTSOutput();
            output.data = input.data;
            output.fields = input.fields;
            output.header_frame_id = null;
            output.header_stamp_nanosec = 0L;
            output.header_stamp_sec = 0;
            output.height = input.height;
            output.is_bigendian = input.is_bigendian;
            output.is_dense = input.is_dense;
            output.point_step = input.point_step;
            output.row_step = input.row_step;
            output .width = input.width;

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