package org.chistera.odom;

import stream.nebula.udf.MapFunction;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ODOM {
    public static class ODOMInput {
        public String child_frame_id;
        public String header;
        public String pose;
        public String twist;
    }

    public static class ODOMOutput {
        public String child_frame_id;
        public String header_frame_id;
        public long header_stamp_nanosec;
        public int header_stamp_sec;
        public String pose_covariance;
        public double pose_pose_orientation_w;
        public double pose_pose_orientation_x;
        public double pose_pose_orientation_y;
        public double pose_pose_orientation_z;
        public double pose_pose_position_x;
        public double pose_pose_position_y;
        public double pose_pose_position_z;
        public String twist_covariance;
        public double twist_twist_angular_x;
        public double twist_twist_angular_y;
        public double twist_twist_angular_z;
        public double twist_twist_linear_x;
        public double twist_twist_linear_y;
        public double twist_twist_linear_z;
    }

    public static class MapODOM implements MapFunction<ODOMInput, ODOMOutput> {
        private static final Pattern FRAME_ID_PATTERN = Pattern.compile("\"frame_id\":\"([^\"]+)\"");
        private static final Pattern NANOSEC_PATTERN = Pattern.compile("\"nanosec\":(-?\\d+)");
        private static final Pattern SEC_PATTERN = Pattern.compile("\"sec\":(-?\\d+)");

        private static final String DOUBLE_REGEX = "(-?\\d+(?:\\.\\d+)?(?:[eE][-+]?\\d+)?)";
        private static final Pattern COVARIANCE_PATTERN = Pattern.compile("\"covariance\":(\\[.*?\\])");
        private static final Pattern POSE_ORIENTATION_PATTERN = Pattern.compile("\"orientation\":\\{\"w\":" + DOUBLE_REGEX + ",\"x\":" + DOUBLE_REGEX + ",\"y\":" + DOUBLE_REGEX + ",\"z\":" + DOUBLE_REGEX + "\\}");
        private static final Pattern POSE_POSITION_PATTERN = Pattern.compile("\"position\":\\{\"x\":" + DOUBLE_REGEX + ",\"y\":" + DOUBLE_REGEX + ",\"z\":" + DOUBLE_REGEX + "\\}");
        private static final Pattern TWIST_ANGULAR_PATTERN = Pattern.compile("\"angular\":\\{\"x\":" + DOUBLE_REGEX + ",\"y\":" + DOUBLE_REGEX + ",\"z\":" + DOUBLE_REGEX + "\\}");
        private static final Pattern TWIST_LINEAR_PATTERN = Pattern.compile("\"linear\":\\{\"x\":" + DOUBLE_REGEX + ",\"y\":" + DOUBLE_REGEX + ",\"z\":" + DOUBLE_REGEX + "\\}");


        @Override
        public ODOMOutput map(final ODOMInput input) {
            ODOMOutput output = new ODOMOutput();
            output.child_frame_id = input.child_frame_id;
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

            if (input.pose != null) {
                Matcher covMatcher = COVARIANCE_PATTERN.matcher(input.pose);
                if (covMatcher.find()) {
                    output.pose_covariance = covMatcher.group(1);
                }

                Matcher oriMatcher = POSE_ORIENTATION_PATTERN.matcher(input.pose);
                if (oriMatcher.find()) {
                    output.pose_pose_orientation_w = Double.parseDouble(oriMatcher.group(1));
                    output.pose_pose_orientation_x = Double.parseDouble(oriMatcher.group(2));
                    output.pose_pose_orientation_y = Double.parseDouble(oriMatcher.group(3));
                    output.pose_pose_orientation_z = Double.parseDouble(oriMatcher.group(4));
                }

                Matcher posMatcher = POSE_POSITION_PATTERN.matcher(input.pose);
                if (posMatcher.find()) {
                    output.pose_pose_position_x = Double.parseDouble(posMatcher.group(1));
                    output.pose_pose_position_y = Double.parseDouble(posMatcher.group(2));
                    output.pose_pose_position_z = Double.parseDouble(posMatcher.group(3));
                }
            }

            if (input.twist != null) {
                Matcher covMatcher = COVARIANCE_PATTERN.matcher(input.twist);
                if (covMatcher.find()) {
                    output.twist_covariance = covMatcher.group(1);
                }

                Matcher angMatcher = TWIST_ANGULAR_PATTERN.matcher(input.twist);
                if (angMatcher.find()) {
                    output.twist_twist_angular_x = Double.parseDouble(angMatcher.group(1));
                    output.twist_twist_angular_y = Double.parseDouble(angMatcher.group(2));
                    output.twist_twist_angular_z = Double.parseDouble(angMatcher.group(3));
                }

                Matcher linMatcher = TWIST_LINEAR_PATTERN.matcher(input.twist);
                if (linMatcher.find()) {
                    output.twist_twist_linear_x = Double.parseDouble(linMatcher.group(1));
                    output.twist_twist_linear_y = Double.parseDouble(linMatcher.group(2));
                    output.twist_twist_linear_z = Double.parseDouble(linMatcher.group(3));
                }
            }
            return output;
        }
    }
}