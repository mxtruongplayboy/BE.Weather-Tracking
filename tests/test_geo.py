"""Geo utility tests — bearing, haversine, speed computation."""
import pytest
from utils.geo import haversine_km, bearing_deg, bearing_to_text, compute_movement


def test_haversine_known_distance():
    # Hanoi (21.028, 105.834) to HCM (10.823, 106.630) ≈ 1140 km
    d = haversine_km(21.028, 105.834, 10.823, 106.630)
    assert 1100 < d < 1200


def test_haversine_same_point():
    assert haversine_km(15.0, 120.0, 15.0, 120.0) == 0.0


def test_bearing_north():
    b = bearing_deg(0, 0, 1, 0)
    assert abs(b - 0.0) < 1.0


def test_bearing_east():
    b = bearing_deg(0, 0, 0, 1)
    assert abs(b - 90.0) < 1.0


def test_bearing_south():
    b = bearing_deg(1, 0, 0, 0)
    assert abs(b - 180.0) < 1.0


def test_bearing_to_text():
    assert bearing_to_text(0) == "N"
    assert bearing_to_text(45) == "NE"
    assert bearing_to_text(90) == "E"
    assert bearing_to_text(180) == "S"
    assert bearing_to_text(270) == "W"
    assert bearing_to_text(315) == "NW"


def test_compute_movement_basic():
    import time
    t1 = 0.0
    t2 = 3600.0   # 1 hour later
    # Move ~111 km north (1 degree latitude)
    dir_deg, dir_text, speed_kt = compute_movement(
        0.0, 0.0, t1,
        1.0, 0.0, t2,
    )
    assert dir_text == "N"
    assert dir_deg is not None and abs(dir_deg) < 2
    # ~111 km/h → ~59.9 kt
    assert speed_kt is not None and 55 < speed_kt < 65


def test_compute_movement_zero_time():
    dir_deg, dir_text, speed_kt = compute_movement(0, 0, 0, 1, 0, 0)
    assert dir_deg is None and dir_text is None and speed_kt is None
