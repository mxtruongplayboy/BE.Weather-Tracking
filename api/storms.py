"""
Storm API endpoints — all follow the spec response schema.
Mobile never calls data sources directly; this API serves cached, normalized data.
"""
import json
import logging
from datetime import datetime, timezone
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from core.config import settings
from core.database import get_db
from core.redis import cache_get, cache_set
from models.schemas import (
    ActiveStormsResponse,
    ConeResponse,
    GeoJSONResponse,
    NearbyResponse,
    NearbyStormEntry,
    StormDetail,
    StormDetailResponse,
    StormSummary,
    TrackPoint,
    TrackResponse,
)
from models.storm_models import (
    CanonicalStormLink,
    SourceSyncLog,
    Storm,
    StormCone,
    StormTrack,
    StormTrackPoint,
)
from utils.geo import haversine_km, distance_to_linestring_km

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/storms", tags=["storms"])
map_router = APIRouter(prefix="/api/v1/map", tags=["map"])

ATTRIBUTION = (
    "Tropical cyclone data sources: NOAA/NHC, Japan Meteorological Agency (JMA), "
    "Joint Typhoon Warning Center (JTWC), NOAA/NCEI IBTrACS. "
    "This app is not affiliated with or endorsed by these agencies."
)

# Basin → primary source for deduplication
BASIN_PRIMARY_SOURCE = {
    "AL": "NHC",
    "EP": "NHC",
    "CP": "NHC",
    "WP": "JMA",
    "IO": "JTWC",
    "SP": "JTWC",
}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _is_stale(dt: Optional[datetime]) -> tuple[bool, Optional[int]]:
    if dt is None:
        return True, None
    delta = (_utcnow() - dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else _utcnow() - dt)
    minutes = int(delta.total_seconds() / 60)
    return minutes > settings.stale_threshold_minutes, minutes


def _storm_to_summary(s: Storm) -> StormSummary:
    return StormSummary(
        id=s.id,
        canonicalId=s.canonical_id,
        name=s.name,
        basin=s.basin,
        source=s.source,
        lat=s.lat,
        lon=s.lon,
        maxWindKt=s.wind_kt,
        pressureHpa=s.pressure_hpa,
        category=s.category,
        movementDirectionText=s.movement_direction_text,
        movementDirectionDeg=s.movement_direction_deg,
        movementSpeedKt=s.movement_speed_kt,
        lastUpdateUtc=s.last_update_utc,
    )


def _deduplicate_active(storms: List[Storm]) -> List[Storm]:
    """
    For West Pacific, if a storm appears in both JMA and JTWC with nearly the
    same position, keep only the primary-source entry.
    Simple approach: group by canonical_id and pick the preferred source.
    """
    by_canonical: dict[str, List[Storm]] = {}
    no_canonical: List[Storm] = []

    for s in storms:
        if s.canonical_id:
            by_canonical.setdefault(s.canonical_id, []).append(s)
        else:
            no_canonical.append(s)

    result: List[Storm] = []
    for cid, group in by_canonical.items():
        if len(group) == 1:
            result.append(group[0])
            continue
        # Pick by basin primary source preference
        basin = group[0].basin or ""
        preferred_source = BASIN_PRIMARY_SOURCE.get(basin, "NHC")
        winner = next((s for s in group if s.source == preferred_source), group[0])
        result.append(winner)

    result.extend(no_canonical)
    return result


# ── GET /active ───────────────────────────────────────────────────────────────

@router.get("/active", response_model=ActiveStormsResponse)
def get_active_storms(db: Session = Depends(get_db)):
    cache_key = "storms:active"
    cached = cache_get(cache_key)
    if cached:
        return cached

    active = db.query(Storm).filter(Storm.is_active == True).all()
    active = _deduplicate_active(active)

    summaries = [_storm_to_summary(s) for s in active]

    # Determine staleness from the most recent sync
    latest_sync = (
        db.query(SourceSyncLog)
        .filter(SourceSyncLog.status == "success")
        .order_by(SourceSyncLog.finished_at.desc())
        .first()
    )
    last_sync_time = latest_sync.finished_at if latest_sync else None
    is_stale, stale_minutes = _is_stale(last_sync_time)

    response = ActiveStormsResponse(
        updatedAt=last_sync_time or _utcnow(),
        count=len(summaries),
        isStale=is_stale,
        staleMinutes=stale_minutes if is_stale else None,
        attribution=ATTRIBUTION,
        storms=summaries,
    )

    result = response.dict()
    cache_set(cache_key, result, ttl=settings.cache_ttl_active_storms)
    return result


# ── GET /{id} ─────────────────────────────────────────────────────────────────

@router.get("/{storm_id}", response_model=StormDetailResponse)
def get_storm_detail(storm_id: UUID, db: Session = Depends(get_db)):
    cache_key = f"storms:detail:{storm_id}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    s = db.query(Storm).filter(Storm.id == storm_id).first()
    if s is None:
        raise HTTPException(status_code=404, detail="Storm not found")

    # Find all sources linked to the same canonical_id
    sources = [s.source]
    if s.canonical_id:
        linked = (
            db.query(CanonicalStormLink)
            .filter(CanonicalStormLink.canonical_id == s.canonical_id)
            .all()
        )
        sources = list({lk.source for lk in linked} | {s.source})

    is_stale, _ = _is_stale(s.last_update_utc)

    detail = StormDetail(
        id=s.id,
        canonicalId=s.canonical_id,
        name=s.name,
        basin=s.basin,
        source=s.source,
        lat=s.lat,
        lon=s.lon,
        maxWindKt=s.wind_kt,
        pressureHpa=s.pressure_hpa,
        category=s.category,
        movementDirectionText=s.movement_direction_text,
        movementDirectionDeg=s.movement_direction_deg,
        movementSpeedKt=s.movement_speed_kt,
        lastUpdateUtc=s.last_update_utc,
        status=s.status,
        isActive=s.is_active,
        primarySource=s.source,
        sources=sources,
        createdAt=s.created_at,
        updatedAt=s.updated_at,
    )

    response = StormDetailResponse(
        storm=detail,
        attribution=ATTRIBUTION,
        isStale=is_stale,
    )

    result = response.dict()
    cache_set(cache_key, result, ttl=settings.cache_ttl_storm_detail)
    return result


# ── GET /{id}/track ───────────────────────────────────────────────────────────

@router.get("/{storm_id}/track", response_model=TrackResponse)
def get_storm_track(
    storm_id: UUID,
    type: str = Query("observed", description="observed | forecast"),
    db: Session = Depends(get_db),
):
    cache_key = f"storms:track:{storm_id}:{type}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    s = db.query(Storm).filter(Storm.id == storm_id).first()
    if s is None:
        raise HTTPException(status_code=404, detail="Storm not found")

    pts = (
        db.query(StormTrackPoint)
        .filter(
            StormTrackPoint.storm_id == storm_id,
            StormTrackPoint.point_type == type,
        )
        .order_by(StormTrackPoint.valid_time_utc)
        .all()
    )

    track_points = [
        TrackPoint(
            forecastHour=p.forecast_hour,
            timeUtc=p.valid_time_utc,
            lat=p.lat,
            lon=p.lon,
            windKt=p.wind_kt,
            pressureHpa=p.pressure_hpa,
            category=p.category,
        )
        for p in pts
    ]

    # GeoJSON LineString
    track_db = (
        db.query(StormTrack)
        .filter(
            StormTrack.storm_id == storm_id,
            StormTrack.track_type == type,
        )
        .first()
    )
    geojson = track_db.geojson if track_db else None

    is_stale, _ = _is_stale(s.last_update_utc)

    response = TrackResponse(
        stormId=storm_id,
        type=type,
        points=track_points,
        geojson=geojson,
        isStale=is_stale,
    )

    result = response.dict()
    cache_set(cache_key, result, ttl=settings.cache_ttl_track)
    return result


# ── GET /{id}/forecast ────────────────────────────────────────────────────────

@router.get("/{storm_id}/forecast", response_model=TrackResponse)
def get_storm_forecast(storm_id: UUID, db: Session = Depends(get_db)):
    return get_storm_track(storm_id, type="forecast", db=db)


# ── GET /{id}/cone ────────────────────────────────────────────────────────────

@router.get("/{storm_id}/cone", response_model=ConeResponse)
def get_storm_cone(storm_id: UUID, db: Session = Depends(get_db)):
    cache_key = f"storms:cone:{storm_id}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    s = db.query(Storm).filter(Storm.id == storm_id).first()
    if s is None:
        raise HTTPException(status_code=404, detail="Storm not found")

    cones_db = (
        db.query(StormCone)
        .filter(StormCone.storm_id == storm_id)
        .order_by(StormCone.forecast_hour)
        .all()
    )

    is_stale, _ = _is_stale(s.last_update_utc)

    response = ConeResponse(
        stormId=storm_id,
        cones=[
            {
                "coneType": c.cone_type,
                "forecastHour": c.forecast_hour,
                "geojson": c.geojson,
            }
            for c in cones_db
        ],
        isStale=is_stale,
    )

    result = response.dict()
    cache_set(cache_key, result, ttl=settings.cache_ttl_cone)
    return result


# ── GET /nearby ───────────────────────────────────────────────────────────────

@router.get("/nearby", response_model=NearbyResponse)
def get_storms_nearby(
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    radiusKm: float = Query(1000, ge=1, le=5000),
    db: Session = Depends(get_db),
):
    active = db.query(Storm).filter(Storm.is_active == True).all()
    active = _deduplicate_active(active)

    entries: List[NearbyStormEntry] = []

    for s in active:
        if s.lat is None or s.lon is None:
            continue

        dist_km = haversine_km(lat, lon, s.lat, s.lon)
        if dist_km > radiusKm:
            continue

        # Distance to forecast track
        forecast_track = (
            db.query(StormTrack)
            .filter(
                StormTrack.storm_id == s.id,
                StormTrack.track_type == "forecast",
            )
            .first()
        )
        dist_to_track = None
        closest_time = None
        max_forecast_wind = None

        if forecast_track and forecast_track.geojson:
            coords = forecast_track.geojson.get("coordinates", [])
            dist_to_track = distance_to_linestring_km(lat, lon, coords)

        # Closest forecast point
        forecast_pts = (
            db.query(StormTrackPoint)
            .filter(
                StormTrackPoint.storm_id == s.id,
                StormTrackPoint.point_type == "forecast",
            )
            .order_by(StormTrackPoint.valid_time_utc)
            .all()
        )
        min_d = float("inf")
        for pt in forecast_pts:
            d = haversine_km(lat, lon, pt.lat, pt.lon)
            if d < min_d:
                min_d = d
                closest_time = pt.valid_time_utc
                max_forecast_wind = pt.wind_kt

        risk, message, actions = _compute_risk(
            dist_km=dist_km,
            dist_to_track=dist_to_track,
            wind_kt=s.wind_kt,
            closest_time=closest_time,
        )

        entries.append(
            NearbyStormEntry(
                storm=_storm_to_summary(s),
                distanceKm=round(dist_km, 1),
                distanceToForecastTrackKm=round(dist_to_track, 1) if dist_to_track is not None else None,
                estimatedClosestTimeUtc=closest_time,
                maxForecastWindKtNearUser=max_forecast_wind,
                riskLevel=risk,
                message=message,
                actions=actions,
            )
        )

    entries.sort(key=lambda e: e.distanceKm)

    return NearbyResponse(
        lat=lat,
        lon=lon,
        radiusKm=radiusKm,
        storms=entries,
        lastUpdateUtc=_utcnow(),
        attribution=ATTRIBUTION,
    )


def _compute_risk(
    dist_km: float,
    dist_to_track: Optional[float],
    wind_kt: Optional[float],
    closest_time: Optional[datetime],
) -> tuple[str, str, List[str]]:
    wind = wind_kt or 0
    track_d = dist_to_track if dist_to_track is not None else dist_km

    risk = "low"

    if track_d <= 100 and wind >= 64:
        risk = "extreme"
    elif track_d <= 250 and wind >= 64:
        risk = "high"
    elif track_d <= 100 and wind >= 34:
        risk = "high"
    elif track_d <= 250 and wind >= 34:
        risk = "medium"
    elif dist_km <= 500:
        risk = "low"

    # Elevate if approach within 24h
    if closest_time:
        hours_away = (_utcnow() - closest_time.replace(tzinfo=timezone.utc)).total_seconds() / 3600
        if abs(hours_away) <= 24 and risk in ("low", "medium"):
            risk = "high" if risk == "medium" else "medium"

    messages = {
        "extreme": f"EXTREME RISK — storm within {int(track_d)} km of forecast track with {int(wind)} kt winds.",
        "high": f"HIGH RISK — storm may approach your area. Max wind {int(wind)} kt.",
        "medium": f"MEDIUM RISK — tropical cyclone {int(dist_km)} km away. Monitor closely.",
        "low": f"LOW RISK — tropical cyclone {int(dist_km)} km away.",
    }

    actions_map = {
        "extreme": ["Evacuate immediately if ordered", "Follow official emergency instructions", "Seek shelter now"],
        "high": ["Prepare emergency kit", "Monitor official forecasts", "Be ready to evacuate"],
        "medium": ["Stay informed", "Review emergency plans", "Monitor local authorities"],
        "low": ["Stay aware", "Check weather updates"],
    }

    return risk, messages[risk], actions_map[risk]


# ── GET /map/storms.geojson ───────────────────────────────────────────────────

@map_router.get("/storms.geojson", response_model=GeoJSONResponse)
def get_storms_geojson(db: Session = Depends(get_db)):
    cache_key = "map:storms_geojson"
    cached = cache_get(cache_key)
    if cached:
        return cached

    active = db.query(Storm).filter(Storm.is_active == True).all()
    active = _deduplicate_active(active)

    features = []

    for s in active:
        # Current position feature
        if s.lat is not None and s.lon is not None:
            features.append(
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [s.lon, s.lat]},
                    "properties": {
                        "id": str(s.id),
                        "name": s.name,
                        "basin": s.basin,
                        "source": s.source,
                        "windKt": s.wind_kt,
                        "pressureHpa": s.pressure_hpa,
                        "category": s.category,
                        "featureType": "current_position",
                    },
                }
            )

        # Forecast track line
        forecast_track = (
            db.query(StormTrack)
            .filter(
                StormTrack.storm_id == s.id,
                StormTrack.track_type == "forecast",
            )
            .first()
        )
        if forecast_track and forecast_track.geojson:
            features.append(
                {
                    "type": "Feature",
                    "geometry": forecast_track.geojson,
                    "properties": {
                        "id": str(s.id),
                        "name": s.name,
                        "featureType": "forecast_track",
                    },
                }
            )

        # Cone polygon
        cone = (
            db.query(StormCone)
            .filter(
                StormCone.storm_id == s.id,
                StormCone.cone_type == "forecast_cone",
            )
            .first()
        )
        if cone and cone.geojson:
            features.append(
                {
                    "type": "Feature",
                    "geometry": cone.geojson.get("features", [{}])[0].get("geometry"),
                    "properties": {
                        "id": str(s.id),
                        "name": s.name,
                        "featureType": "forecast_cone",
                    },
                }
            )

    is_stale, _ = _is_stale(
        db.query(SourceSyncLog)
        .filter(SourceSyncLog.status == "success")
        .order_by(SourceSyncLog.finished_at.desc())
        .with_entities(SourceSyncLog.finished_at)
        .scalar()
    )

    response = GeoJSONResponse(
        type="FeatureCollection",
        features=features,
        updatedAt=_utcnow(),
        isStale=is_stale,
    )

    result = response.dict()
    cache_set(cache_key, result, ttl=settings.cache_ttl_geojson)
    return result
