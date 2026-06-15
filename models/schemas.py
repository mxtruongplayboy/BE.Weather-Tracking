from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel


# ── Active storms list ────────────────────────────────────────────────────────

class StormSummary(BaseModel):
    id: UUID
    canonicalId: Optional[str] = None
    name: Optional[str] = None
    basin: Optional[str] = None
    source: str
    lat: Optional[float] = None
    lon: Optional[float] = None
    maxWindKt: Optional[float] = None
    pressureHpa: Optional[float] = None
    category: Optional[str] = None
    movementDirectionText: Optional[str] = None
    movementDirectionDeg: Optional[float] = None
    movementSpeedKt: Optional[float] = None
    lastUpdateUtc: Optional[datetime] = None
    isActive: bool = True

    class Config:
        from_attributes = True


class ActiveStormsResponse(BaseModel):
    updatedAt: datetime
    count: int
    isStale: bool = False
    staleMinutes: Optional[int] = None
    attribution: str = (
        "Tropical cyclone data sources: NOAA/NHC, Japan Meteorological Agency (JMA), "
        "Joint Typhoon Warning Center (JTWC), NOAA/NCEI IBTrACS. "
        "This app is not affiliated with or endorsed by these agencies."
    )
    storms: List[StormSummary]


# ── Storm detail ──────────────────────────────────────────────────────────────

class StormDetail(StormSummary):
    status: Optional[str] = None
    isActive: bool = True
    primarySource: str
    sources: List[str] = []
    createdAt: Optional[datetime] = None
    updatedAt: Optional[datetime] = None


class StormDetailResponse(BaseModel):
    storm: StormDetail
    attribution: str = (
        "Tropical cyclone data sources: NOAA/NHC, Japan Meteorological Agency (JMA), "
        "Joint Typhoon Warning Center (JTWC), NOAA/NCEI IBTrACS. "
        "This app is not affiliated with or endorsed by these agencies."
    )
    isStale: bool = False


# ── Track (observed or forecast) ──────────────────────────────────────────────

class TrackPoint(BaseModel):
    forecastHour: Optional[int] = None
    timeUtc: Optional[datetime] = None
    lat: float
    lon: float
    windKt: Optional[float] = None
    pressureHpa: Optional[float] = None
    category: Optional[str] = None
    movementDirectionDeg: Optional[float] = None
    movementDirectionText: Optional[str] = None
    movementSpeedKt: Optional[float] = None


class TrackResponse(BaseModel):
    stormId: UUID
    type: str               # observed | forecast
    points: List[TrackPoint]
    geojson: Optional[Dict[str, Any]] = None
    isStale: bool = False


# ── Cone ─────────────────────────────────────────────────────────────────────

class ConeResponse(BaseModel):
    stormId: UUID
    cones: List[Dict[str, Any]]
    isStale: bool = False


# ── Nearby ───────────────────────────────────────────────────────────────────

class NearbyStormEntry(BaseModel):
    storm: StormSummary
    distanceKm: float
    distanceToForecastTrackKm: Optional[float] = None
    estimatedClosestTimeUtc: Optional[datetime] = None
    maxForecastWindKtNearUser: Optional[float] = None
    riskLevel: str          # low | medium | high | extreme
    message: str
    actions: List[str] = []


class NearbyResponse(BaseModel):
    lat: float
    lon: float
    radiusKm: float
    storms: List[NearbyStormEntry]
    lastUpdateUtc: datetime
    attribution: str = (
        "Tropical cyclone data sources: NOAA/NHC, Japan Meteorological Agency (JMA), "
        "Joint Typhoon Warning Center (JTWC), NOAA/NCEI IBTrACS. "
        "This app is not affiliated with or endorsed by these agencies."
    )


# ── GeoJSON map layer ─────────────────────────────────────────────────────────

class GeoJSONResponse(BaseModel):
    type: str = "FeatureCollection"
    features: List[Dict[str, Any]]
    updatedAt: datetime
    isStale: bool = False


# ── Health ────────────────────────────────────────────────────────────────────

class SourceHealthEntry(BaseModel):
    source: str
    lastSuccess: Optional[datetime] = None
    lastError: Optional[datetime] = None
    lastErrorMessage: Optional[str] = None
    isStale: bool = False
    staleMinutes: Optional[int] = None


class HealthResponse(BaseModel):
    status: str
    sources: List[SourceHealthEntry]
    checkedAt: datetime


# ── Lightning events ──────────────────────────────────────────────────────────

LIGHTNING_ATTRIBUTION = (
    "Lightning and storm data sources may include NOAA GOES Geostationary Lightning Mapper (GLM), "
    "EUMETSAT Meteosat Third Generation Lightning Imager (MTG LI), NASA LIS/OTD historical lightning datasets, "
    "NOAA/NCEP Global Forecast System (GFS), and tropical cyclone sources used by the storm module. "
    "This app is not affiliated with or endorsed by NOAA, NASA, EUMETSAT, JMA, JTWC, or any other data provider. "
    "Forecasts and risk estimates are for informational purposes and should not replace official warnings."
)


class LightningEventOut(BaseModel):
    id: str
    source: str
    eventType: str
    timeUtc: datetime
    lat: float
    lon: float
    energy: Optional[float] = None
    quality: Optional[float] = None
    satellite: Optional[str] = None


class RecentLightningResponse(BaseModel):
    updatedAt: datetime
    coverageNote: str
    attribution: str = LIGHTNING_ATTRIBUTION
    events: List[LightningEventOut]
    isStale: bool = False


# ── Lightning risk ────────────────────────────────────────────────────────────

class RiskTimelineEntry(BaseModel):
    validTimeUtc: datetime
    forecastHour: int
    riskScore: float
    riskLevel: str            # low | moderate | high | very_high
    message: str
    cape_jkg: Optional[float] = None
    convective_precip_mm: Optional[float] = None


class LightningRiskPointResponse(BaseModel):
    location: Dict[str, float]
    updatedAt: datetime
    source: str
    attribution: str = LIGHTNING_ATTRIBUTION
    timeline: List[RiskTimelineEntry]
    isStale: bool = False


# ── Hazards summary ───────────────────────────────────────────────────────────

class LightningHazardSummary(BaseModel):
    realStrikeAvailable: bool
    riskLevel: Optional[str] = None
    riskScore: Optional[float] = None
    message: str
    coverageNote: str


class HazardsSummaryResponse(BaseModel):
    location: Dict[str, float]
    radiusKm: float
    updatedAt: datetime
    storms: List[Any]
    lightning: LightningHazardSummary
    attribution: str = LIGHTNING_ATTRIBUTION
