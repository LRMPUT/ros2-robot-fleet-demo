package org.chistera.timestamp;

import stream.nebula.udf.MapFunction;
import java.time.Instant;

public class Timestamp {
    public static class TimestampInput {
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

    public static class TimestampOutput {
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
        public String t2_ns;
    }

    public static class MapTimestamp implements MapFunction<TimestampInput, TimestampOutput> {

        @Override
        public TimestampOutput map(final TimestampInput input) {

            TimestampOutput output = new TimestampOutput();
            output.altitude = input.altitude;
            output.header_frame_id = input.header_frame_id;
            output.header_stamp_sec = input.header_stamp_sec;
            output.header_stamp_nanosec = input.header_stamp_nanosec;
            output.latitude = input.latitude;
            output.longitude = input.longitude;
            output.position_covariance = input.position_covariance;
            output.position_covariance_type = input.position_covariance_type;
            output.status_service = input.status_service;
            output.status_status = input.status_status;
            output.t0_ns = input.t0_ns;
            output.t1_ns = input.t1_ns;
            output.exited = input.exited;
            Instant now = Instant.now();
            output.t2_ns = String.valueOf((now.getEpochSecond() * 1_000_000_000L) + now.getNano());
            return output;
        }
    }
}