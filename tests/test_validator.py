from csb_validator.validator_crowbar import run_custom_validation

import pytest
from datetime import datetime, timedelta, timezone
import tempfile
import json
import os
import sys
import os

def create_feature(coords, depth=None, heading=None, time=None):
    return {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": coords
        },
        "properties": {
            "depth": depth,
            "heading": heading,
            "time": time
        }
    }

def write_temp_geojson(features, processing=None):
    data = {
        "type": "FeatureCollection",
        "features": features
    }
    if processing:
        data["properties"] = {"processing": processing}
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".geojson", mode="w", encoding="utf-8")
    json.dump(data, tmp)
    tmp.close()
    return tmp.name


def run_validation_and_cleanup(file_path):
    _, errors = run_custom_validation(file_path)
    os.unlink(file_path)
    return errors


def test_valid_feature():
    past_time = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    f = create_feature([10.0, 10.0], 100, 90, past_time)
    path = write_temp_geojson([f])
    errors = run_validation_and_cleanup(path)
    assert errors == []


def test_missing_coordinates():
    f = create_feature(None, 100, 90, "2020-01-01T00:00:00Z")
    path = write_temp_geojson([f])
    errors = run_validation_and_cleanup(path)
    assert any("Invalid geometry coordinates" in e["error"] for e in errors)


def test_out_of_bounds_coordinates():
    f = create_feature([200.0, -100.0], 100, 90, "2020-01-01T00:00:00Z")
    path = write_temp_geojson([f])
    errors = run_validation_and_cleanup(path)
    assert any("Longitude out of bounds" in e["error"] for e in errors)
    assert any("Latitude out of bounds" in e["error"] for e in errors)


def test_missing_depth():
    f = create_feature([0.0, 0.0], None, 90, "2020-01-01T00:00:00Z")
    path = write_temp_geojson([f])
    errors = run_validation_and_cleanup(path)
    assert any("Depth cannot be blank" in e["error"] for e in errors)


def test_heading_not_numeric():
    f = create_feature([0.0, 0.0], 100, "not-a-number", "2020-01-01T00:00:00Z")
    path = write_temp_geojson([f])
    errors = run_validation_and_cleanup(path)
    assert any("Heading is not a valid number" in e["error"] for e in errors)


def test_heading_out_of_bounds():
    f = create_feature([0.0, 0.0], 100, 400, "2020-01-01T00:00:00Z")
    path = write_temp_geojson([f])
    errors = run_validation_and_cleanup(path)
    assert any("Heading out of bounds" in e["error"] for e in errors)


def test_missing_timestamp():
    f = create_feature([0.0, 0.0], 100, 90, None)
    path = write_temp_geojson([f])
    errors = run_validation_and_cleanup(path)
    assert any("Timestamp cannot be blank" in e["error"] for e in errors)


def test_future_timestamp():
    future_time = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    f = create_feature([0.0, 0.0], 100, 90, future_time)
    path = write_temp_geojson([f])
    errors = run_validation_and_cleanup(path)
    assert any("Timestamp should be in the past" in e["error"] for e in errors)


def test_invalid_timestamp_format():
    f = create_feature([0.0, 0.0], 100, 90, "invalid-date")
    path = write_temp_geojson([f])
    errors = run_validation_and_cleanup(path)
    assert any("Invalid ISO 8601 timestamp" in e["error"] for e in errors)


def test_global_processing_timestamp_future():
    f = create_feature([0.0, 0.0], 100, 90, "2020-01-01T00:00:00Z")
    path = write_temp_geojson([f], processing=[{"timestamp": "2999-01-01T00:00:00Z"}])
    errors = run_validation_and_cleanup(path)
    assert any("Timestamp should be in the past" in e["error"] for e in errors)
