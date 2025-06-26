import json
from datetime import datetime, timezone
from typing import List, Dict, Tuple, Any
from csb_validator.line_mapper import map_feature_property_lines_sync, map_feature_coordinates_line_sync

def run_custom_validation(file_path: str) -> Tuple[str, List[Dict[str, Any]]]:
    errors = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        line_map = map_feature_property_lines_sync(file_path)
        coord_line_map = map_feature_coordinates_line_sync(file_path)
        for i, feature in enumerate(data.get("features", [])):
            props = feature.get("properties", {})
            coords = feature.get("geometry", {}).get("coordinates", [])
            line_num = coord_line_map.get(i, i + 1)
            if not coords or not isinstance(coords, list) or len(coords) < 2:
                errors.append({"file": file_path, "line": line_num, "error": "Invalid geometry coordinates"})
                continue
            lon, lat = coords[0], coords[1]
            if lon is None or lon < -180 or lon > 180:
                errors.append({"file": file_path, "line": line_num, "error": f"Longitude out of bounds: {lon}"})
            if lat is None or lat < -90 or lat > 90:
                errors.append({"file": file_path, "line": line_num, "error": f"Latitude out of bounds: {lat}"})
            depth = props.get("depth")
            if depth is None:
                depth_line = line_map.get((i, "depth"), line_num)
                errors.append({"file": file_path, "line": depth_line, "error": "Depth cannot be blank"})
            heading = props.get("heading")
            if heading is not None:
                try:
                    heading_val = float(heading)
                    if heading_val < 0 or heading_val > 360:
                        heading_line = line_map.get((i, "heading"), line_num)
                        errors.append({"file": file_path, "line": heading_line, "error": f"Heading out of bounds: {heading}"})
                except ValueError:
                    heading_line = line_map.get((i, "heading"), line_num)
                    errors.append({"file": file_path, "line": heading_line, "error": f"Heading is not a valid number: {heading}"})
            time_str = props.get("time")
            if not time_str:
                time_line = line_map.get((i, "time"), line_num)
                errors.append({"file": file_path, "line": time_line, "error": "Timestamp cannot be blank"})
            else:
                try:
                    timestamp = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
                    now = datetime.now(timezone.utc)
                    if timestamp > now:
                        time_line = line_map.get((i, "time"), line_num)
                        errors.append({"file": file_path, "line": time_line, "error": f"Timestamp should be in the past: {time_str[:10]}"})
                except Exception:
                    time_line = line_map.get((i, "time"), line_num)
                    errors.append({"file": file_path, "line": time_line, "error": f"Invalid ISO 8601 timestamp: {time_str}"})
        processing = data.get("properties", {}).get("processing", [])
        if isinstance(processing, list):
            with open(file_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
            for p in processing:
                global_time = p.get("timestamp")
                if global_time:
                    time_line = next((i + 1 for i, line in enumerate(lines) if global_time in line), "N/A")
                    try:
                        timestamp = datetime.fromisoformat(global_time.replace("Z", "+00:00"))
                        now = datetime.now(timezone.utc)
                        if timestamp > now:
                            errors.append({"file": file_path, "line": time_line, "error": f"Timestamp should be in the past: {global_time[:10]}"})
                    except Exception:
                        errors.append({"file": file_path, "line": time_line, "error": f"Invalid ISO 8601 timestamp: {global_time}"})
    except Exception as e:
        errors.append({"file": file_path, "line": "N/A", "error": f"Failed to parse JSON: {str(e)}"})
    return file_path, errors
