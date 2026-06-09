"""
Lightning API endpoints.

GET /api/v1/lightning/events/recent  — recent GOES/MTG strike events by bbox/window
GET /api/v1/lightning/risk/point     — GFS risk timeline at a location
GET /api/v1/tiles/lightning-risk/{z}/{x}/{y}.png  — PNG heatmap tile
GET /api/v1/tiles/lightning-events/{z}/{x}/{y}.pbf — (stub, reserved for production PBF)

Mobile app must never call NOAA/EUMETSAT/NASA directly.
"""
import io
import logging
import math
from datetime import datetime, timedelta, timezone
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from sqlalchemy.orm import Session

from core.config import settings
from core.database import get_db
from core.redis import cache_get, cache_set
from models.lightning_models import LightningEvent, LightningRiskForecast
from models.schemas import (
    LightningEventOut,
    LightningRiskPointResponse,
    RecentLightningResponse,
    RiskTimelineEntry,
    LIGHTNING_ATTRIBUTION,
)
from crawlers.gfs_risk_worker import query_risk_at_point, RISK_MESSAGES

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/lightning", tags=["lightning"])
tiles_router = APIRouter(prefix="/api/v1/tiles", tags=["tiles"])

# GOES satellite coverage bbox (approximate): lon -160..0, lat -65..65
GOES_COVERAGE_LON_MIN = -160.0
GOES_COVERAGE_LON_MAX = 15.0
GOES_COVERAGE_LAT_MIN = -65.0
GOES_COVERAGE_LAT_MAX = 65.0

# MTG LI approximate coverage: lon -65..65, lat -65..65
MTG_COVERAGE_LON_MIN = -65.0
MTG_COVERAGE_LON_MAX = 65.0


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _bbox_in_goes_coverage(min_lon: float, max_lon: float,
                            min_lat: float, max_lat: float) -> bool:
    return (min_lon <= GOES_COVERAGE_LON_MAX and max_lon >= GOES_COVERAGE_LON_MIN
            and min_lat <= GOES_COVERAGE_LAT_MAX and max_lat >= GOES_COVERAGE_LAT_MIN)


def _point_has_real_strike(lat: float, lon: float) -> bool:
    return _bbox_in_goes_coverage(lon, lon, lat, lat)


def _tile_to_bbox(z: int, x: int, y: int) -> tuple[float, float, float, float]:
    """Convert TMS tile coords to (min_lon, min_lat, max_lon, max_lat)."""
    n = 2 ** z
    lon_min = x / n * 360.0 - 180.0
    lon_max = (x + 1) / n * 360.0 - 180.0
    lat_max = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    lat_min = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))
    return lon_min, lat_min, lon_max, lat_max


# ── Risk level → RGBA color ───────────────────────────────────────────────────

_RISK_COLORS = {
    "very_high": (220, 20, 20, 200),    # red
    "high":      (255, 140, 0, 180),    # orange
    "moderate":  (255, 215, 0, 150),    # yellow
    "low":       (0, 200, 80, 80),      # green (mostly transparent)
}


def _risk_png_tile(z: int, x: int, y: int, db: Session) -> bytes:
    """
    Generate a 256×256 PNG heatmap tile for lightning risk.
    Each pixel represents ~RESOLUTION/256 degrees of the tile.
    """
    try:
        from PIL import Image, ImageDraw
        import numpy as np
    except ImportError:
        raise HTTPException(status_code=503, detail="Pillow/numpy not installed for tile rendering")

    TILE_SIZE = 256
    lon_min, lat_min, lon_max, lat_max = _tile_to_bbox(z, x, y)

    # Find the latest GFS run
    latest_run = (
        db.query(LightningRiskForecast.run_time_utc)
        .filter(LightningRiskForecast.model_source == "GFS")
        .order_by(LightningRiskForecast.run_time_utc.desc())
        .first()
    )
    if latest_run is None:
        # Return transparent tile
        img = Image.new("RGBA", (TILE_SIZE, TILE_SIZE), (0, 0, 0, 0))
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue()

    run_dt = latest_run[0]

    # Fetch grid points in tile bbox for current valid time (nearest to now)
    now = _utcnow()
    rows = (
        db.query(LightningRiskForecast)
        .filter(
            LightningRiskForecast.model_source == "GFS",
            LightningRiskForecast.run_time_utc == run_dt,
            LightningRiskForecast.lat.between(lat_min - 1, lat_max + 1),
            LightningRiskForecast.lon.between(lon_min - 1, lon_max + 1),
            LightningRiskForecast.valid_time_utc <= now + timedelta(hours=6),
        )
        .order_by(LightningRiskForecast.forecast_hour)
        .all()
    )

    img = Image.new("RGBA", (TILE_SIZE, TILE_SIZE), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    lon_range = lon_max - lon_min
    lat_range = lat_max - lat_min

    for row in rows:
        if lon_range == 0 or lat_range == 0:
            continue
        px = int((row.lon - lon_min) / lon_range * TILE_SIZE)
        py = int((lat_max - row.lat) / lat_range * TILE_SIZE)
        color = _RISK_COLORS.get(row.risk_level, (0, 0, 0, 0))
        # Draw a small filled circle per grid point, scaled with zoom
        radius = max(2, min(20, int(TILE_SIZE / (2 ** max(0, 7 - z)))))
        draw.ellipse([px - radius, py - radius, px + radius, py + radius], fill=color)

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/events/recent", response_model=RecentLightningResponse)
def get_recent_lightning_events(
    bbox: Optional[str] = Query(None, description="minLon,minLat,maxLon,maxLat"),
    sinceMinutes: int = Query(30, ge=5, le=60),
    sources: Optional[str] = Query(None, description="Comma-separated: GOES_GLM,MTG_LI"),
    db: Session = Depends(get_db),
):
    """Return recent real lightning events from GOES GLM and/or MTG LI."""
    # Parse bbox
    min_lon = max_lon = min_lat = max_lat = None
    if bbox:
        try:
            parts = [float(v) for v in bbox.split(",")]
            min_lon, min_lat, max_lon, max_lat = parts
        except Exception:
            raise HTTPException(status_code=400, detail="bbox must be minLon,minLat,maxLon,maxLat")

    # Parse sources filter
    source_filter = []
    if sources:
        source_filter = [s.strip() for s in sources.split(",")]
    else:
        source_filter = ["GOES_GLM", "MTG_LI"]

    cache_key = f"lightning:events:recent:{','.join(sorted(source_filter))}:{sinceMinutes}"
    if min_lon is not None:
        cache_key += f":{min_lon:.1f},{min_lat:.1f},{max_lon:.1f},{max_lat:.1f}"

    cached = cache_get(cache_key)
    if cached:
        return RecentLightningResponse(**cached)

    since = _utcnow() - timedelta(minutes=sinceMinutes)

    q = db.query(LightningEvent).filter(
        LightningEvent.source.in_(source_filter),
        LightningEvent.time_utc >= since,
    )
    if min_lon is not None:
        q = q.filter(
            LightningEvent.lat.between(min_lat, max_lat),
            LightningEvent.lon.between(min_lon, max_lon),
        )
    q = q.order_by(LightningEvent.time_utc.desc()).limit(500)
    events = q.all()

    # Determine coverage note
    has_goes_coverage = (
        min_lon is None or _bbox_in_goes_coverage(min_lon, max_lon, min_lat, max_lat)
    )
    if has_goes_coverage:
        coverage_note = (
            "Real lightning strike data available for Americas and adjacent oceans (NOAA GOES GLM). "
            "Outside this region, only lightning risk forecast is available."
        )
    else:
        coverage_note = (
            "Live strike data unavailable for this region; showing lightning risk instead. "
            "Real data available only within GOES/MTG satellite coverage."
        )

    event_list = [
        LightningEventOut(
            id=str(ev.id),
            source=ev.source,
            eventType=ev.event_type,
            timeUtc=ev.time_utc,
            lat=ev.lat,
            lon=ev.lon,
            energy=ev.energy,
            quality=ev.quality,
            satellite=ev.satellite,
        )
        for ev in events
    ]

    response = RecentLightningResponse(
        updatedAt=_utcnow(),
        coverageNote=coverage_note,
        events=event_list,
    )

    cache_set(cache_key, response.dict(), ttl=settings.cache_ttl_lightning_events)
    return response


@router.get("/risk/point", response_model=LightningRiskPointResponse)
def get_lightning_risk_at_point(
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    hours: int = Query(24, ge=1, le=120),
):
    """Return GFS-based lightning risk forecast timeline for a location."""
    cache_key = f"lightning:risk:point:{lat:.2f}:{lon:.2f}:{hours}"
    cached = cache_get(cache_key)
    if cached:
        return LightningRiskPointResponse(**cached)

    timeline_raw = query_risk_at_point(lat, lon, hours=hours)

    if not timeline_raw:
        raise HTTPException(
            status_code=404,
            detail="No lightning risk data available for this location. GFS data may not be ingested yet.",
        )

    timeline = [
        RiskTimelineEntry(
            validTimeUtc=entry["validTimeUtc"],
            forecastHour=entry["forecastHour"],
            riskScore=entry["riskScore"],
            riskLevel=entry["riskLevel"],
            message=entry["message"],
            cape_jkg=entry.get("cape_jkg"),
            convective_precip_mm=entry.get("convective_precip_mm"),
        )
        for entry in timeline_raw
    ]

    response = LightningRiskPointResponse(
        location={"lat": lat, "lon": lon},
        updatedAt=_utcnow(),
        source="GFS_RISK",
        timeline=timeline,
    )

    cache_set(cache_key, response.dict(), ttl=settings.cache_ttl_lightning_risk)
    return response


@tiles_router.get("/lightning-risk/{z}/{x}/{y}.png")
def lightning_risk_tile_png(
    z: int, x: int, y: int,
    db: Session = Depends(get_db),
):
    """Generate PNG heatmap tile for lightning risk layer."""
    cache_key = f"lightning:risk:tile:png:{z}:{x}:{y}"
    cached_bytes = cache_get(cache_key)
    if cached_bytes and isinstance(cached_bytes, str):
        import base64
        png_data = base64.b64decode(cached_bytes)
        return Response(content=png_data, media_type="image/png")

    png_data = _risk_png_tile(z, x, y, db)

    import base64
    cache_set(cache_key, base64.b64encode(png_data).decode(), ttl=settings.cache_ttl_lightning_tile)

    return Response(
        content=png_data,
        media_type="image/png",
        headers={"Cache-Control": "public, max-age=900"},
    )


@tiles_router.get("/lightning-events/{z}/{x}/{y}.pbf")
def lightning_events_tile_pbf(z: int, x: int, y: int, db: Session = Depends(get_db)):
    """
    Lightning events vector tile (PBF). MVP returns GeoJSON wrapper.
    In production, use a proper vector tile server (pg_tileserv, tippecanoe).
    """
    lon_min, lat_min, lon_max, lat_max = _tile_to_bbox(z, x, y)
    since = _utcnow() - timedelta(minutes=60)

    events = (
        db.query(LightningEvent)
        .filter(
            LightningEvent.time_utc >= since,
            LightningEvent.lat.between(lat_min, lat_max),
            LightningEvent.lon.between(lon_min, lon_max),
        )
        .limit(200)
        .all()
    )

    features = [
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [ev.lon, ev.lat]},
            "properties": {
                "id": str(ev.id),
                "source": ev.source,
                "type": ev.event_type,
                "time": ev.time_utc.isoformat(),
                "energy": ev.energy,
            },
        }
        for ev in events
    ]

    import json
    return Response(
        content=json.dumps({"type": "FeatureCollection", "features": features}),
        media_type="application/json",
        headers={"Cache-Control": "public, max-age=60"},
    )
