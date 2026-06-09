import math
from typing import Optional, Tuple


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate great-circle distance in km between two WGS84 points."""
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate initial bearing in degrees (0–360) from point1 to point2."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dlambda = math.radians(lon2 - lon1)
    x = math.sin(dlambda) * math.cos(phi2)
    y = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(dlambda)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def bearing_to_text(deg: float) -> str:
    """Convert bearing degrees to 8-point compass direction."""
    directions = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    idx = round(deg / 45) % 8
    return directions[idx]


def compute_movement(
    lat1: float,
    lon1: float,
    time1_unix: float,
    lat2: float,
    lon2: float,
    time2_unix: float,
) -> Tuple[Optional[float], Optional[str], Optional[float]]:
    """
    Compute movement direction and speed between two track points.
    Returns (direction_deg, direction_text, speed_kt).
    """
    time_hours = (time2_unix - time1_unix) / 3600.0
    if time_hours <= 0:
        return None, None, None

    dist_km = haversine_km(lat1, lon1, lat2, lon2)
    speed_kmh = dist_km / time_hours
    speed_kt = speed_kmh * 0.539957
    dir_deg = bearing_deg(lat1, lon1, lat2, lon2)
    dir_text = bearing_to_text(dir_deg)

    return round(dir_deg, 1), dir_text, round(speed_kt, 1)


def distance_to_linestring_km(
    lat: float, lon: float, coordinates: list
) -> float:
    """
    Approximate minimum distance from a point to a LineString (list of [lon, lat]).
    Returns distance in km.
    """
    if not coordinates:
        return float("inf")
    min_dist = float("inf")
    for coord in coordinates:
        d = haversine_km(lat, lon, coord[1], coord[0])
        if d < min_dist:
            min_dist = d
    return min_dist
