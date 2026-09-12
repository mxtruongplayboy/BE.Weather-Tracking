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
from crawlers.mtg_li_worker import is_configured as mtg_is_configured

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
MTG_COVERAGE_LAT_MIN = -65.0
MTG_COVERAGE_LAT_MAX = 65.0


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _bbox_in_goes_coverage(min_lon: float, max_lon: float,
                            min_lat: float, max_lat: float) -> bool:
    return (min_lon <= GOES_COVERAGE_LON_MAX and max_lon >= GOES_COVERAGE_LON_MIN
            and min_lat <= GOES_COVERAGE_LAT_MAX and max_lat >= GOES_COVERAGE_LAT_MIN)


def _bbox_in_mtg_coverage(min_lon: float, max_lon: float,
                          min_lat: float, max_lat: float) -> bool:
    return (min_lon <= MTG_COVERAGE_LON_MAX and max_lon >= MTG_COVERAGE_LON_MIN
            and min_lat <= MTG_COVERAGE_LAT_MAX and max_lat >= MTG_COVERAGE_LAT_MIN)


def _real_strike_sources(min_lon: float, max_lon: float,
                         min_lat: float, max_lat: float) -> list[str]:
    """
    Vệ tinh nào thật sự phủ khung nhìn này VÀ đang bật.

    Trước đây chỉ hỏi mỗi GOES, nên MTG_COVERAGE_* nằm đó không ai dùng: kể cả
    khi đã điền credential EUMETSAT và sự kiện MTG LI đã nằm trong DB, người
    dùng châu Âu/châu Phi vẫn bị trả lời "vùng này không có dữ liệu thật".

    MTG chỉ được tính khi có credential — không thì hứa suông một nguồn đang tắt.
    """
    sources = []
    if _bbox_in_goes_coverage(min_lon, max_lon, min_lat, max_lat):
        sources.append("GOES GLM")
    if mtg_is_configured() and _bbox_in_mtg_coverage(min_lon, max_lon, min_lat, max_lat):
        sources.append("MTG LI")
    return sources


def _point_has_real_strike(lat: float, lon: float) -> bool:
    return bool(_real_strike_sources(lon, lon, lat, lat))


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
    "very_high": (220, 30, 30, 210),    # red — highly opaque
    "high":      (255, 130, 0, 190),    # orange
    "moderate":  (255, 210, 0, 160),    # yellow
    # "low" intentionally omitted — rendered as transparent (no tile fill)
}


def _risk_png_tile(z: int, x: int, y: int, db: Session) -> bytes:
    """
    Generate a 256×256 PNG heatmap tile for lightning risk (GFS 1° grid).

    Design decisions:
    - Only render moderate / high / very_high — skip "low" (too noisy, near-invisible anyway).
    - Select the single forecast hour whose valid_time is closest to now.
    - Fill the entire 1° grid cell rather than drawing a circle, so coverage
      looks solid at all zoom levels (no sparse dots at high zoom).
    """
    try:
        from PIL import Image, ImageDraw
    except ImportError:
        raise HTTPException(status_code=503, detail="Pillow not installed for tile rendering")

    TILE_SIZE = 256
    RESOLUTION_DEG = 1.0  # GFS grid resolution

    def _transparent() -> bytes:
        buf = io.BytesIO()
        Image.new("RGBA", (TILE_SIZE, TILE_SIZE), (0, 0, 0, 0)).save(buf, format="PNG")
        return buf.getvalue()

    lon_min, lat_min, lon_max, lat_max = _tile_to_bbox(z, x, y)

    # Find the latest GFS run
    latest_run = (
        db.query(LightningRiskForecast.run_time_utc)
        .filter(LightningRiskForecast.model_source == "GFS")
        .order_by(LightningRiskForecast.run_time_utc.desc())
        .first()
    )
    if latest_run is None:
        return _transparent()

    run_dt = latest_run[0]

    # Pick the forecast hour whose valid_time is closest to now.
    # This avoids stacking all hours on top of each other.
    now = _utcnow()
    best_fhour_row = (
        db.query(LightningRiskForecast.forecast_hour, LightningRiskForecast.valid_time_utc)
        .filter(
            LightningRiskForecast.model_source == "GFS",
            LightningRiskForecast.run_time_utc == run_dt,
        )
        .distinct(LightningRiskForecast.forecast_hour)
        .all()
    )
    if not best_fhour_row:
        return _transparent()

    best_fhour = min(
        best_fhour_row,
        key=lambda r: abs((r.valid_time_utc.replace(tzinfo=None) - now.replace(tzinfo=None)).total_seconds()),
    ).forecast_hour

    # Pad bbox by 1 cell so grid points near the tile edge are included
    pad = RESOLUTION_DEG
    rows = (
        db.query(LightningRiskForecast)
        .filter(
            LightningRiskForecast.model_source == "GFS",
            LightningRiskForecast.run_time_utc == run_dt,
            LightningRiskForecast.forecast_hour == best_fhour,
            LightningRiskForecast.lat.between(lat_min - pad, lat_max + pad),
            LightningRiskForecast.lon.between(lon_min - pad, lon_max + pad),
            # Only render meaningful risk — "low" is too faint and creates noise
            LightningRiskForecast.risk_level.in_(["moderate", "high", "very_high"]),
        )
        .all()
    )

    if not rows:
        return _transparent()

    img = Image.new("RGBA", (TILE_SIZE, TILE_SIZE), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    lon_range = lon_max - lon_min
    lat_range = lat_max - lat_min
    if lon_range == 0 or lat_range == 0:
        return _transparent()

    # Pixels per degree in this tile
    px_per_deg_lon = TILE_SIZE / lon_range
    px_per_deg_lat = TILE_SIZE / lat_range

    # Cell size in pixels (1° grid → fill the whole cell)
    cell_w = max(2, int(RESOLUTION_DEG * px_per_deg_lon))
    cell_h = max(2, int(RESOLUTION_DEG * px_per_deg_lat))

    for row in rows:
        color = _RISK_COLORS.get(row.risk_level)
        if color is None:
            continue
        # Centre of the grid cell
        cx = int((row.lon - lon_min) * px_per_deg_lon)
        cy = int((lat_max - row.lat) * px_per_deg_lat)
        # Fill the entire 1° cell (half-cell in each direction)
        x0 = cx - cell_w // 2
        y0 = cy - cell_h // 2
        x1 = cx + cell_w // 2
        y1 = cy + cell_h // 2
        draw.rectangle([x0, y0, x1, y1], fill=color)

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
    if min_lon is None:
        # Truy vấn toàn cầu: liệt kê mọi nguồn đang bật, không nói về một vùng nào.
        covering = ["GOES GLM"] + (["MTG LI"] if mtg_is_configured() else [])
    else:
        covering = _real_strike_sources(min_lon, max_lon, min_lat, max_lat)

    if covering:
        coverage_note = (
            f"Real lightning strike data available for this area via {' + '.join(covering)}. "
            "Outside satellite coverage, only lightning risk forecast is available."
        )
    else:
        coverage_note = (
            "Live strike data unavailable for this region; showing lightning risk instead. "
            "No geostationary lightning imager currently covers it."
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


@router.get("/risk-zones")
def get_lightning_risk_zones(
    bbox: Optional[str] = Query(None, description="minLon,minLat,maxLon,maxLat"),
    min_risk: float = Query(0.55, ge=0.3, le=1.0),
    limit: int = Query(400, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    """
    Public endpoint: return current GFS high-risk grid points for mobile clustering.
    Mobile uses these as cluster markers (zoom-in = spread, zoom-out = group).
    Only moderate/high/very_high risk zones are returned.
    """
    cache_key = f"lightning:risk:zones:{min_risk}:{bbox or 'global'}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    latest_run = (
        db.query(LightningRiskForecast.run_time_utc)
        .filter(LightningRiskForecast.model_source == "GFS")
        .order_by(LightningRiskForecast.run_time_utc.desc())
        .first()
    )
    if not latest_run:
        return {"runTime": None, "count": 0, "points": []}

    q = db.query(LightningRiskForecast).filter(
        LightningRiskForecast.model_source == "GFS",
        LightningRiskForecast.run_time_utc == latest_run[0],
        LightningRiskForecast.forecast_hour == 0,
        LightningRiskForecast.risk_score >= min_risk,
    )
    if bbox:
        try:
            min_lon, min_lat, max_lon, max_lat = [float(v) for v in bbox.split(",")]
            q = q.filter(
                LightningRiskForecast.lat.between(min_lat, max_lat),
                LightningRiskForecast.lon.between(min_lon, max_lon),
            )
        except Exception:
            raise HTTPException(status_code=400, detail="bbox must be minLon,minLat,maxLon,maxLat")

    rows = q.order_by(LightningRiskForecast.risk_score.desc()).limit(limit).all()

    result = {
        "runTime": latest_run[0].isoformat(),
        "count": len(rows),
        "points": [
            {
                "lat": r.lat,
                "lon": r.lon,
                "riskScore": round(r.risk_score, 2),
                "riskLevel": r.risk_level,
                "capeJkg": r.cape_jkg,
            }
            for r in rows
        ],
    }
    cache_set(cache_key, result, ttl=settings.cache_ttl_lightning_risk)
    return result


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
