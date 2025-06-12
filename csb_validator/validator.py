import json
import os
import sys
import asyncio
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple
import aiofiles


def validate_feature(feature: Dict[str, Any], index: int) -> Tuple[int, List[str]]:
    errors = []
    geometry = feature.get("geometry", {})
    properties = feature.get("properties", {})

    coords = geometry.get("coordinates")
    if not coords or not isinstance(coords, list) or len(coords) < 2:
        errors.append("Invalid geometry coordinates")
    else:
        lon, lat = coords[0], coords[1]
        if lon is None:
            errors.append("Longitude cannot be blank")
        elif lon < -180:
            errors.append("Longitude should be ≥ -180")
        elif lon > 180:
            errors.append("Longitude should be ≤ 180")

        if lat is None:
            errors.append("Latitude cannot be blank")
        elif lat < -90:
            errors.append("Latitude should be ≥ -90")
        elif lat > 90:
            errors.append("Latitude should be ≤ 90")

    depth = properties.get("depth")
    if depth is None:
        errors.append("Depth cannot be blank")

    heading = properties.get("heading")
    if heading is not None:
        if heading < 0:
            errors.append("Heading should be ≥ 0")
        elif heading > 360:
            errors.append("Heading should be ≤ 360")

    time_str = properties.get("time")
    if not time_str:
        errors.append("Timestamp cannot be blank")
    else:
        try:
            timestamp = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
            now_utc = datetime.now(timezone.utc)
            if timestamp >= now_utc:
                errors.append("Timestamp should be in the past")
        except ValueError:
            errors.append("Invalid timestamp format")

    return index, errors


def validate_geojson(data: Dict[str, Any]) -> List[Tuple[int, List[str]]]:
    errors_per_feature = []
    features = data.get("features", [])
    for i, feature in enumerate(features):
        index, errors = validate_feature(feature, i)
        if errors:
            errors_per_feature.append((index, errors))
    return errors_per_feature


async def validate_file(path: str) -> Tuple[str, List[Tuple[int, List[str]]], str]:
    try:
        async with aiofiles.open(path, "r") as f:
            contents = await f.read()
            data = json.loads(contents)
            results = validate_geojson(data)
            return path, results, "success"
    except Exception as e:
        return path, [(0, [f"Error: {str(e)}"])], "error"


async def run_validation(paths: List[str]):
    tasks = [validate_file(path) for path in paths]
    results = await asyncio.gather(*tasks)

    print("\n📋 Validation Report:\n" + "="*40)

    for path, feature_errors, status in results:
        filename = os.path.basename(path)
        if status == "error":
            print(f"\n❌ {filename}: Failed to process")
            for _, errs in feature_errors:
                for err in errs:
                    print(f"  - {err}")
        elif not feature_errors:
            print(f"\n✅ {filename}: All features passed validation.")
        else:
            print(f"\n❌ {filename}: {len(feature_errors)} feature(s) with issues.")
            for index, errs in feature_errors:
                print(f"  Feature #{index}:")
                for err in errs:
                    print(f"    - {err}")


def main():
    if len(sys.argv) < 2:
        print("Usage: csb-validator file1.geojson [file2.geojson ...]")
        sys.exit(1)
    asyncio.run(run_validation(sys.argv[1:]))


if __name__ == "__main__":
    main()
