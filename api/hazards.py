"""
Unified hazards summary endpoint — Sprint 6.

GET /api/v1/hazards/summary?lat=16.05&lon=108.20&radiusKm=500

Returns nearby storms + lightning risk in one request, reducing mobile app round-trips.
"""
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from core.config import settings
from core.database import get_db
from core.redis import cache_get, cache_set
from models.lightning_models import LightningEvent, LightningRiskForecast
from models.schemas import HazardsSummaryResponse, LightningHazardSummary, LIGHTNING_ATTRIBUTION
from crawlers.gfs_risk_worker import query_risk_at_point, RISK_MESSAGES

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/hazards", tags=["hazards"])

GOES_LON_MIN, GOES_LON_MAX = -160.0, 15.0
GOES_LAT_MIN, GOES_LAT_MAX = -65.0, 65.0


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _has_real_strike_coverage(lat: float, lon: float) -> bool:
    return (GOES_LON_MIN <= lon <= GOES_LON_MAX and GOES_LAT_MIN <= lat <= GOES_LAT_MAX)


def _get_nearby_storms(lat: float, lon: float, radius_km: float, db: Session) -> list[dict]:
    """Return simplified storm summaries near the given point."""
    from models.storm_models import Storm
    from utils.geo import haversine_km

    active_storms = db.query(Storm).filter(Storm.is_active == True).all()
    nearby = []
    for s in active_storms:
        if s.lat is None or s.lon is None:
            continue
        dist = haversine_km(lat, lon, s.lat, s.lon)
        if dist <= radius_km:
            nearby.append({
                "id": str(s.id),
                "name": s.name,
                "basin": s.basin,
                "category": s.category,
                "lat": s.lat,
                "lon": s.lon,
                "distanceKm": round(dist, 1),
                "windKt": s.wind_kt,
                "source": s.source,
            })
    nearby.sort(key=lambda x: x["distanceKm"])
    return nearby


def _get_lightning_summary(lat: float, lon: float, db: Session) -> LightningHazardSummary:
    """Build lightning hazard summary for a location."""
    has_real = _has_real_strike_coverage(lat, lon)

    # Check for real strikes within 30 km in last 30 min
    real_strike_nearby = False
    if has_real:
        since = _utcnow() - timedelta(minutes=30)
        nearby_strike = (
            db.query(LightningEvent)
            .filter(
                LightningEvent.time_utc >= since,
                LightningEvent.lat.between(lat - 0.3, lat + 0.3),
                LightningEvent.lon.between(lon - 0.3, lon + 0.3),
            )
            .first()
        )
        real_strike_nearby = nearby_strike is not None

    # GFS risk for next 6 hours
    timeline = query_risk_at_point(lat, lon, hours=6)
    if timeline:
        best = max(timeline, key=lambda t: t["riskScore"])
        risk_level = best["riskLevel"]
        risk_score = best["riskScore"]
        message = best["message"]
    else:
        risk_level = None
        risk_score = None
        message = "Lightning risk data not yet available."

    if real_strike_nearby:
        message = "Lightning detected nearby. Seek shelter immediately."
    elif risk_level == "very_high":
        message = "Very high lightning risk. Avoid open water and exposed areas."
    elif risk_level == "high":
        message = "High thunderstorm and lightning risk. Consider stopping outdoor/sea activity."

    if has_real:
        coverage_note = "Real lightning strike data available for this region (NOAA GOES GLM)."
    else:
        coverage_note = (
            "Live strike data unavailable here; showing lightning risk instead. "
            "Real-time lightning detection requires GOES/MTG satellite coverage."
        )

    return LightningHazardSummary(
        realStrikeAvailable=real_strike_nearby,
        riskLevel=risk_level,
        riskScore=risk_score,
        message=message,
        coverageNote=coverage_note,
    )


@router.get("/summary", response_model=HazardsSummaryResponse)
def get_hazards_summary(
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    radiusKm: float = Query(500, ge=50, le=2000),
    db: Session = Depends(get_db),
):
    """
    Unified hazards summary: nearby storms + lightning risk.
    Designed to reduce mobile app round-trips.
    """
    cache_key = f"hazards:summary:{lat:.2f}:{lon:.2f}:{radiusKm:.0f}"
    cached = cache_get(cache_key)
    if cached:
        return HazardsSummaryResponse(**cached)

    storms = _get_nearby_storms(lat, lon, radiusKm, db)
    lightning = _get_lightning_summary(lat, lon, db)

    response = HazardsSummaryResponse(
        location={"lat": lat, "lon": lon},
        radiusKm=radiusKm,
        updatedAt=_utcnow(),
        storms=storms,
        lightning=lightning,
    )

    cache_set(cache_key, response.dict(), ttl=settings.cache_ttl_hazards_summary)
    return response
