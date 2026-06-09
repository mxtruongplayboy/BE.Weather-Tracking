"""Unit conversion tests — spec requirement."""
import pytest
from utils.units import ms_to_kt, kmh_to_kt, mph_to_kt, kt_to_ms, categorize_storm


def test_ms_to_kt():
    assert abs(ms_to_kt(1.0) - 1.94384) < 0.001
    assert abs(ms_to_kt(51.44) - 100.0) < 0.1  # ~100 kt typhoon


def test_kmh_to_kt():
    assert abs(kmh_to_kt(185.0) - 99.89) < 0.1   # strong typhoon


def test_mph_to_kt():
    assert abs(mph_to_kt(115) - 99.93) < 0.1


def test_kt_to_ms_roundtrip():
    kt = 65.0
    assert abs(kt_to_ms(ms_to_kt(kt_to_ms(kt))) - kt_to_ms(kt)) < 0.001


def test_categorize_storm_wp():
    assert categorize_storm(110, "WP") == "violent_typhoon"
    assert categorize_storm(70, "WP") == "typhoon"
    assert categorize_storm(50, "WP") == "severe_tropical_storm"
    assert categorize_storm(38, "WP") == "tropical_storm"
    assert categorize_storm(25, "WP") == "tropical_depression"


def test_categorize_storm_al():
    assert categorize_storm(140, "AL") == "category_5"
    assert categorize_storm(115, "AL") == "category_4"
    assert categorize_storm(100, "AL") == "category_3"
    assert categorize_storm(85, "AL") == "category_2"
    assert categorize_storm(65, "AL") == "category_1"
    assert categorize_storm(40, "AL") == "tropical_storm"
    assert categorize_storm(25, "AL") == "tropical_depression"
