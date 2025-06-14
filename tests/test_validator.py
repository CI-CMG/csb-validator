import pytest
from datetime import datetime, timedelta, timezone
from csb_validator.validator import validate_feature, validate_geojson


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


def test_valid_feature():
    past_time = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    feature = create_feature([10.0, 10.0], 100, 90, past_time)
    _, errors = validate_feature(feature, 0)
    assert errors == []


def test_missing_coordinates():
    feature = create_feature(None, 50, 180, "2020-01-01T00:00:00Z")
    _, errors = validate_feature(feature, 0)
    assert "Invalid geometry coordinates" in errors


def test_invalid_long_lat():
    feature = create_feature([200.0, 100.0], 50, 180, "2020-01-01T00:00:00Z")
    _, errors = validate_feature(feature, 0)
    assert "Longitude should be ≤ 180" in errors
    assert "Latitude should be ≤ 90" in errors


def test_missing_depth():
    feature = create_feature([10.0, 10.0], None, 90, "2020-01-01T00:00:00Z")
    _, errors = validate_feature(feature, 0)
    assert "Depth cannot be blank" in errors


def test_heading_out_of_range():
    feature = create_feature([10.0, 10.0], 100, 400, "2020-01-01T00:00:00Z")
    _, errors = validate_feature(feature, 0)
    assert "Heading should be ≤ 360" in errors


def test_missing_timestamp():
    feature = create_feature([10.0, 10.0], 100, 90, None)
    _, errors = validate_feature(feature, 0)
    assert "Timestamp cannot be blank" in errors


def test_future_timestamp():
    future_time = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    feature = create_feature([10.0, 10.0], 100, 90, future_time)
    _, errors = validate_feature(feature, 0)
    assert "Timestamp should be in the past" in errors


def test_invalid_timestamp_format():
    feature = create_feature([10.0, 10.0], 100, 90, "not-a-date")
    _, errors = validate_feature(feature, 0)
    assert "Invalid timestamp format" in errors

def test_minimum_valid_long_lat():
    feature = create_feature([-180.0, -90.0], 10, 0, "2020-01-01T00:00:00Z")
    _, errors = validate_feature(feature, 0)
    assert errors == []

def test_maximum_valid_long_lat():
    feature = create_feature([180.0, 90.0], 10, 360, "2020-01-01T00:00:00Z")
    _, errors = validate_feature(feature, 0)
    assert errors == []

def test_null_longitude():
    feature = create_feature([None, 45.0], 10, 0, "2020-01-01T00:00:00Z")
    _, errors = validate_feature(feature, 0)
    assert "Longitude cannot be blank" in errors

def test_null_latitude():
    feature = create_feature([30.0, None], 10, 0, "2020-01-01T00:00:00Z")
    _, errors = validate_feature(feature, 0)
    assert "Latitude cannot be blank" in errors

def test_negative_heading():
    feature = create_feature([10.0, 10.0], 10, -5, "2020-01-01T00:00:00Z")
    _, errors = validate_feature(feature, 0)
    assert "Heading should be ≥ 0" in errors

def test_missing_properties():
    feature = {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
        "properties": {}
    }
    _, errors = validate_feature(feature, 0)
    assert "Depth cannot be blank" in errors
    assert "Timestamp cannot be blank" in errors

def test_empty_feature_collection():
    data = {
        "type": "FeatureCollection",
        "features": []
    }
    errors = validate_geojson(data)
    assert errors == []

def test_missing_features_key():
    data = {
        "type": "FeatureCollection"
        # No 'features' key
    }
    errors = validate_geojson(data)
    assert errors == []

def test_non_list_coordinates():
    feature = {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": "not-a-list"},
        "properties": {
            "depth": 100,
            "heading": 90,
            "time": "2020-01-01T00:00:00Z"
        }
    }
    _, errors = validate_feature(feature, 0)
    assert "Invalid geometry coordinates" in errors

def test_too_few_coordinates():
    feature = {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [30.0]},
        "properties": {
            "depth": 100,
            "heading": 90,
            "time": "2020-01-01T00:00:00Z"
        }
    }
    _, errors = validate_feature(feature, 0)
    assert "Invalid geometry coordinates" in errors



def test_validate_geojson_multiple_features():
    features = [
        create_feature([10.0, 10.0], 100, 90, "2020-01-01T00:00:00Z"), 
        create_feature([200.0, 10.0], 100, 90, "2020-01-01T00:00:00Z"), 
    ]
    data = {
        "type": "FeatureCollection",
        "features": features
    }
    errors = validate_geojson(data)
    assert len(errors) == 1
    assert errors[0][0] == 1
    assert "Longitude should be ≤ 180" in errors[0][1]
