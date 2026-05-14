from pathlib import Path
from typing import Dict, Optional
import binascii
from shapely import wkb
from shapely.geometry import Point, Polygon
from rosbags.rosbag2 import Reader
from rosbags.typesys import Stores, get_typestore
import logging

logger = logging.getLogger("uvicorn.info")

class ROSBagGeofenceReader:
    """ROSbag reader for geofencing"""
    
    def __init__(self, bag_directory: str):
        self.bag_path = Path(bag_directory)
        if not self.bag_path.exists():
            raise FileNotFoundError(f"ROSbag not found: {bag_directory}")
        
        self.typestore = get_typestore(Stores.LATEST)
    
    def get_gps_outside_zone(
        self, 
        robot_id: str, 
        polygon_hex: str,
        limit: Optional[int] = None
    ) -> Dict:
        """
        Returns points outside of bounds

        Args:
            robot_id: leader
            polygon_hex: PolygonHex
            limit: Limit of returned points (None = returning all)
        
        Returns:
            Dict with points list and metadata
        """
        topic_name = f"/{robot_id}/gps/fix"
        
        logger.info(f"Reading ROSbag: {self.bag_path}")
        logger.info(f"Topic: {topic_name}")
        
        try:
            polygon = self._parse_polygon(polygon_hex)
        except Exception as e:
            raise ValueError(f"Invalid polygon HEX: {e}")
        
        points_outside = []
        total_points = 0
        start_time = None
        end_time = None
        first_outside_time = None
        last_outside_time = None
        
        with Reader(self.bag_path) as reader:
            connections = [x for x in reader.connections if x.topic == topic_name]
            
            if not connections:
                raise ValueError(f"Topic {topic_name} not found in ROSbag")
            
            for connection, timestamp, rawdata in reader.messages(connections=connections):
                msg = self.typestore.deserialize_cdr(rawdata, connection.msgtype)
                
                # Timestamp
                try:
                    msg_sec = msg.header.stamp.sec
                    msg_nanosec = msg.header.stamp.nanosec
                    ts_sec = msg_sec + (msg_nanosec / 1e9)
                except AttributeError:
                    ts_sec = timestamp / 1e9
                
                # Start/end time for all points
                if start_time is None:
                    start_time = ts_sec
                end_time = ts_sec
                
                # GPS coords
                lat = msg.latitude
                lon = msg.longitude
                point = Point(lon, lat)
                
                total_points += 1
                
                if not polygon.contains(point):
                    # Track point times that are outside separately
                    if first_outside_time is None:
                        first_outside_time = ts_sec
                    last_outside_time = ts_sec
                    
                    points_outside.append({
                        "timestamp": ts_sec,
                        "latitude": lat,
                        "longitude": lon
                    })
                    
                    # Limit check, stops only adding to the list
                    if limit and len(points_outside) >= limit:
                        logger.info(f"Reached limit of {limit} points (continuing scan for stats)")
                        # continue scanning for statistics
        
        duration = end_time - start_time if start_time else 0
        
        # if there was a limit, limit the list (but the metadata will be in full)
        returned_points = points_outside[:limit] if limit else points_outside
        
        return {
            "robot_id": robot_id,
            "total_points": total_points,
            "points_outside": returned_points,
            "count_outside": len(points_outside),
            "duration_seconds": round(duration, 2),
            "start_time": start_time,
            "end_time": end_time,
            "first_violation_time": first_outside_time,
            "last_violation_time": last_outside_time
        }
    
    def _parse_polygon(self, hex_string: str) -> Polygon:
        """Parse WKB HEX to shapely polygon"""
        polygons = []
        for hex_part in hex_string.split("|"):
            if not hex_part.strip():
                continue
            
            try:
                binary_data = binascii.unhexlify(hex_part.strip())
                binary_data = self._clean_ewkb(binary_data)
                geom = wkb.loads(binary_data)
                polygons.append(geom)
            except Exception as e:
                logger.warning(f"Failed to parse polygon part: {e}")
                continue
        
        if not polygons:
            raise ValueError("No valid polygons found")
        
        return polygons[0]
    
    def _clean_ewkb(self, bytes_data: bytes) -> bytes:
        """Delete SRID from PostGIS EWKB format"""
        if len(bytes_data) < 5:
            return bytes_data
        
        is_little_endian = (bytes_data[0] == 1)
        
        if is_little_endian:
            wkb_type = int.from_bytes(bytes_data[1:5], 'little')
        else:
            wkb_type = int.from_bytes(bytes_data[1:5], 'big')
        
        if wkb_type & 0x20000000:
            new_bytes = bytearray(bytes_data[:5])
            
            new_type = wkb_type & ~0x20000000
            if is_little_endian:
                new_bytes[1:5] = new_type.to_bytes(4, 'little')
            else:
                new_bytes[1:5] = new_type.to_bytes(4, 'big')
            
            new_bytes.extend(bytes_data[9:])
            return bytes(new_bytes)
        
        return bytes_data