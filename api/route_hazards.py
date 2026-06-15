"""
Internal route-hazards endpoint — for AI BE use only.

GET /api/v1/internal/route-hazards
  ?points=[[lat,lon],[lat,lon],...]   (JSON array, max 50 points)
  &radius_km=50                       (default 50, max 200)
  &lightning_window_min=30            (default 30)

Returns storm intersections + lightning counts along the route.
AI BE uses this to compute storm_track_intersects_route and lightning_near_route flags.
"""
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from core.database import get_db
from core.redis import cache_get, cache_set
from models.lightning_models import LightningEvent, LightningRiskForecast
from models.storm_models import Storm, StormTrack, StormTrackPoint
from utils.geo import haversine_km, distance_to_linestring_km
from crawlers.gfs_risk_worker import query_risk_at_point

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/internal", tags=["internal"])


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _route_bbox(points: list[list[float]]) -> dict[str, float]:
    lats = [p[0] for p in points]
    lons = [p[1] for p in points]
    return {
        "min_lat": min(lats),
        "max_lat": max(lats),
        "min_lon": min(lons),
        "max_lon": max(lons),
    }


def _bbox_pad(bbox: dict, pad_deg: float) -> dict:
    return {
        "min_lat": bbox["min_lat"] - pad_deg,
        "max_lat": bbox["max_lat"] + pad_deg,
        "min_lon": bbox["min_lon"] - pad_deg,
        "max_lon": bbox["max_lon"] + pad_deg,
    }


def _min_dist_point_to_route(lat: float, lon: float, points: list) -> float:
    """Minimum haversine distance from a point to any segment of the route."""
    min_d = float("inf")
    for p in points:
        d = haversine_km(lat, lon, p[0], p[1])
        if d < min_d:
            min_d = d
    return min_d


def _storm_severity(wind_kt: Optional[float], dist_km: float) -> str:
    if dist_km <= 0:
        dist_km = 1
    if wind_kt and wind_kt >= 96:  # Category 3+
        if dist_km <= 200:
            return "critical"
        return "high"
    if wind_kt and wind_kt >= 64:  # Category 1+
        if dist_km <= 150:
            return "high"
        return "medium"
    if dist_km <= 100:
        return "medium"
    return "low"


@router.get("/route-hazards")
def get_route_hazards(
    points: str = Query(..., description="JSON array [[lat,lon],...], max 50 points"),
    radius_km: float = Query(50.0, ge=10, le=200),
    lightning_window_min: int = Query(30, ge=10, le=120),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """
    Check storm and lightning hazards along a route.
    Designed for AI BE marine risk computation.
    """
    try:
        route_points: list[list[float]] = json.loads(points)
    except (json.JSONDecodeError, ValueError):
        raise HTTPException(status_code=422, detail="points must be a valid JSON array [[lat,lon],...]")

    if not route_points or len(route_points) < 1:
        raise HTTPException(status_code=422, detail="points array must have at least 1 point")
    if len(route_points) > 50:
        raise HTTPException(status_code=422, detail="points array must have at most 50 points")

    # Validate each point
    for i, p in enumerate(route_points):
        if len(p) != 2:
            raise HTTPException(status_code=422, detail=f"Point {i} must be [lat, lon]")
        lat, lon = p
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            raise HTTPException(status_code=422, detail=f"Point {i} has invalid coordinates")

    # Cache key based on route + params
    import hashlib
    cache_key = f"route_hazards:{hashlib.md5(points.encode()).hexdigest()}:{radius_km}:{lightning_window_min}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    bbox = _route_bbox(route_points)
    # Pad bbox by radius_km converted to approximate degrees (1 deg ≈ 111 km)
    pad_deg = radius_km / 111.0
    padded = _bbox_pad(bbox, pad_deg)

    # ── Storms ────────────────────────────────────────────────────────────────
    active_storms = db.query(Storm).filter(Storm.is_active == True).all()
    storm_intersects = []

    for storm in active_storms:
        # Check current storm position against route
        if storm.lat is not None and storm.lon is not None:
            dist_current = _min_dist_point_to_route(storm.lat, storm.lon, route_points)
        else:
            dist_current = float("inf")

        # Check forecast track against route
        dist_forecast = float("inf")
        intersects_forecast = False

        forecast_points = (
            db.query(StormTrackPoint)
            .filter(
                StormTrackPoint.storm_id == storm.id,
                StormTrackPoint.point_type == "forecast",
            )
            .all()
        )
        for fp in forecast_points:
            d = _min_dist_point_to_route(fp.lat, fp.lon, route_points)
            if d < dist_forecast:
                dist_forecast = d
            if d <= radius_km:
                intersects_forecast = True

        # Also check storm tracks GeoJSON
        storm_tracks = (
            db.query(StormTrack)
            .filter(
                StormTrack.storm_id == storm.id,
                StormTrack.track_type == "forecast",
            )
            .all()
        )
        for track in storm_tracks:
            if track.geojson and track.geojson.get("coordinates"):
                coords = track.geojson["coordinates"]  # [[lon,lat],...]
                d = distance_to_linestring_km(
                    route_points[0][0], route_points[0][1], coords
                )
                if d < dist_forecast:
                    dist_forecast = d
                if d <= radius_km:
                    intersects_forecast = True

        closest_dist = min(dist_current, dist_forecast)
        if closest_dist > radius_km * 2 and not intersects_forecast:
            continue

        severity = _storm_severity(storm.wind_kt, closest_dist)
        if severity == "low" and closest_dist > radius_km:
            continue

        storm_intersects.append({
            "storm_id": str(storm.id),
            "canonical_id": storm.canonical_id,
            "storm_name": storm.name,
            "basin": storm.basin,
            "storm_category": storm.category,
            "wind_kt": storm.wind_kt,
            "current_lat": storm.lat,
            "current_lon": storm.lon,
            "closest_distance_km": round(closest_dist, 1),
            "intersects_forecast_track": intersects_forecast,
            "severity": severity,
        })

    storm_intersects.sort(key=lambda x: x["closest_distance_km"])

    # ── Lightning strikes (real — GOES GLM) ───────────────────────────────────
    since = _utcnow() - timedelta(minutes=lightning_window_min)
    lightning_nearby = False
    lightning_count = 0

    # Bbox query for lightning events along the route
    lat_pad = radius_km / 111.0
    lon_pad = radius_km / (111.0 * max(0.1, abs(
        sum(p[0] for p in route_points) / len(route_points)
    ) / 90 * 0.8 + 0.2))

    events = (
        db.query(LightningEvent)
        .filter(
            LightningEvent.time_utc >= since,
            LightningEvent.lat.between(padded["min_lat"], padded["max_lat"]),
            LightningEvent.lon.between(padded["min_lon"], padded["max_lon"]),
        )
        .all()
    )
    for ev in events:
        d = _min_dist_point_to_route(ev.lat, ev.lon, route_points)
        if d <= radius_km:
            lightning_count += 1
            lightning_nearby = True

    # ── GFS lightning risk along route (for areas outside GOES coverage) ──────
    max_risk_score = 0.0
    max_risk_level = None
    # Sample up to 5 points evenly along route
    sample_step = max(1, len(route_points) // 5)
    sample_points = route_points[::sample_step]
    for sp in sample_points:
        timeline = query_risk_at_point(sp[0], sp[1], hours=6)
        if timeline:
            best = max(timeline, key=lambda t: t["riskScore"])
            if best["riskScore"] > max_risk_score:
                max_risk_score = best["riskScore"]
                max_risk_level = best["riskLevel"]

    max_storm_severity = "none"
    if storm_intersects:
        sev_order = {"low": 1, "medium": 2, "high": 3, "critical": 4}
        max_storm_severity = max(
            storm_intersects, key=lambda s: sev_order.get(s["severity"], 0)
        )["severity"]

    result = {
        "checked_at": _utcnow().isoformat(),
        "route_bbox": bbox,
        "radius_km": radius_km,
        "storm_nearby": len(storm_intersects) > 0,
        "storm_intersects": storm_intersects,
        "max_storm_severity": max_storm_severity,
        "lightning_nearby": lightning_nearby,
        "lightning_count_in_window": lightning_count,
        "lightning_window_min": lightning_window_min,
        "lightning_risk_level": max_risk_level,
        "lightning_risk_score": round(max_risk_score, 2),
    }

    cache_set(cache_key, result, ttl=120)  # 2-minute cache
    return result
