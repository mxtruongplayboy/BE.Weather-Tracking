"""
ORM models for the Lightning Module.
Tables: lightning_sources, lightning_events, lightning_risk_forecasts.
Shares the same PostgreSQL/PostGIS DB as the Storm module.
"""
import uuid

from sqlalchemy import (
    Boolean, Column, DateTime, Float, Integer, String, Text, UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.sql import func

from core.database import Base


class LightningSource(Base):
    __tablename__ = "lightning_sources"

    id = Column(Integer, primary_key=True)
    code = Column(String(50), unique=True, nullable=False)   # GOES_GLM, MTG_LI, NASA_LIS, GFS_RISK
    name = Column(Text, nullable=False)
    source_type = Column(String(50), nullable=False)         # strike, historical, model_risk
    base_url = Column(Text)
    license_note = Column(Text)
    coverage_note = Column(Text)
    enabled = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class LightningEvent(Base):
    __tablename__ = "lightning_events"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source = Column(String(50), nullable=False)              # GOES_GLM, MTG_LI
    source_event_id = Column(String(200))
    satellite = Column(String(50))                           # GOES_EAST, GOES_WEST, MTG_I1
    product = Column(String(100))                            # GLM-L2-LCFA, LI-L2-LGR
    event_type = Column(String(50), nullable=False)          # flash, group, event
    time_utc = Column(DateTime(timezone=True), nullable=False, index=True)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    energy = Column(Float)
    area_km2 = Column(Float)
    duration_ms = Column(Float)
    quality = Column(Float)
    detection_count = Column(Integer)
    raw = Column(JSONB)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("source", "source_event_id", "time_utc", name="uq_lightning_event"),
    )


class LightningRiskForecast(Base):
    __tablename__ = "lightning_risk_forecasts"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    model_source = Column(String(50), nullable=False, index=True)   # GFS, ICON, ECMWF_OPEN
    run_time_utc = Column(DateTime(timezone=True), nullable=False)
    valid_time_utc = Column(DateTime(timezone=True), nullable=False, index=True)
    forecast_hour = Column(Integer, nullable=False)
    grid_id = Column(String(100))
    resolution_deg = Column(Float)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    risk_score = Column(Float, nullable=False)               # 0.0 - 1.0
    risk_level = Column(String(30), nullable=False)          # low, moderate, high, very_high
    cape_jkg = Column(Float)
    convective_precip_mm = Column(Float)
    precip_rate_mmh = Column(Float)
    lifted_index = Column(Float)
    cloud_top_temp_c = Column(Float)
    wind_shear_ms = Column(Float)
    raw = Column(JSONB)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint(
            "model_source", "run_time_utc", "valid_time_utc", "lat", "lon",
            name="uq_lightning_risk",
        ),
    )


# Source seed data
LIGHTNING_SOURCES = [
    {
        "code": "GOES_GLM",
        "name": "NOAA GOES Geostationary Lightning Mapper",
        "source_type": "strike",
        "base_url": "s3://noaa-goes16/GLM-L2-LCFA/",
        "license_note": "NOAA open data - free for public use",
        "coverage_note": "Americas and adjacent oceans (GOES-East/West coverage)",
    },
    {
        "code": "MTG_LI",
        "name": "EUMETSAT MTG Lightning Imager",
        "source_type": "strike",
        "base_url": "https://api.eumetsat.int/",
        "license_note": "EUMETSAT Data Policy - requires registration",
        "coverage_note": "Europe, Africa, Middle East and surrounding areas",
    },
    {
        "code": "NASA_LIS",
        "name": "NASA Lightning Imaging Sensor / OTD Historical",
        "source_type": "historical",
        "base_url": "https://ghrc.nsstc.nasa.gov/lightning/",
        "license_note": "NASA EOSDIS open science data",
        "coverage_note": "Global historical lightning climatology (not realtime)",
    },
    {
        "code": "GFS_RISK",
        "name": "NOAA GFS Lightning Risk Model",
        "source_type": "model_risk",
        "base_url": "https://nomads.ncep.noaa.gov/",
        "license_note": "NOAA open data - free for public use",
        "coverage_note": "Global (0-360° lon, 90 to -90° lat) at 1° resolution",
    },
]
