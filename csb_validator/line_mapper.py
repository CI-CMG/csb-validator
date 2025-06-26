def map_feature_property_lines_sync(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    feature_prop_line_map = {}
    in_features = False
    feature_index = -1
    open_braces = 0
    for i, line in enumerate(lines):
        stripped = line.strip()
        if not in_features and '"features"' in stripped and "[" in stripped:
            in_features = True
            continue
        if in_features:
            if "{" in line and open_braces == 0:
                feature_index += 1
            open_braces += line.count("{")
            open_braces -= line.count("}")
            for prop in ["time", "depth", "heading"]:
                if f'"{prop}"' in line:
                    feature_prop_line_map[(feature_index, prop)] = i + 1
            if "]" in line and open_braces == 0:
                break
    return feature_prop_line_map

def map_feature_coordinates_line_sync(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    coord_line_map = {}
    in_features = False
    feature_index = -1
    open_braces = 0
    for i, line in enumerate(lines):
        stripped = line.strip()
        if not in_features and '"features"' in stripped and "[" in stripped:
            in_features = True
            continue
        if in_features:
            if "{" in line and open_braces == 0:
                feature_index += 1
            open_braces += line.count("{")
            open_braces -= line.count("}")
            if '"coordinates"' in stripped:
                coord_line_map[feature_index] = i + 1
            if "]" in line and open_braces == 0:
                break
    return coord_line_map
