import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean, Column, DateTime, Float, ForeignKey,
    Integer, String, Text, UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from core.database import Base


def utcnow():
    return datetime.now(timezone.utc)


class StormSource(Base):
    __tablename__ = "storm_sources"

    id = Column(Integer, primary_key=True)
    code = Column(String(20), unique=True, nullable=False)   # NHC, JMA, JTWC, IBTrACS
    name = Column(String(100))
    base_url = Column(String(500))
    license_note = Column(Text)
    enabled = Column(Boolean, default=True)


class Storm(Base):
    __tablename__ = "storms"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    canonical_id = Column(String(50), index=True, nullable=True)   # e.g. WP-2026-04
    source = Column(String(20), nullable=False)                    # NHC | JMA | JTWC | IBTrACS
    source_storm_id = Column(String(50), index=True)               # original ID from source
    name = Column(String(100), nullable=True)
    basin = Column(String(10), nullable=True)                      # AL, EP, CP, WP, IO, SP
    status = Column(String(50), nullable=True)                     # active, dissipated
    is_active = Column(Boolean, default=True, index=True)

    lat = Column(Float, nullable=True)
    lon = Column(Float, nullable=True)
    wind_kt = Column(Float, nullable=True)
    pressure_hpa = Column(Float, nullable=True)
    movement_direction_deg = Column(Float, nullable=True)
    movement_direction_text = Column(String(5), nullable=True)
    movement_speed_kt = Column(Float, nullable=True)
    category = Column(String(50), nullable=True)

    last_update_utc = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    track_points = relationship("StormTrackPoint", back_populates="storm", cascade="all, delete-orphan")
    tracks = relationship("StormTrack", back_populates="storm", cascade="all, delete-orphan")
    cones = relationship("StormCone", back_populates="storm", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("source", "source_storm_id", name="uq_source_storm"),
    )


class StormTrackPoint(Base):
    __tablename__ = "storm_track_points"

    id = Column(Integer, primary_key=True)
    storm_id = Column(UUID(as_uuid=True), ForeignKey("storms.id", ondelete="CASCADE"), nullable=False, index=True)
    point_type = Column(String(20), nullable=False)     # observed | forecast
    forecast_hour = Column(Integer, nullable=True)      # 0, 12, 24, 36, 48, 72, 96, 120
    valid_time_utc = Column(DateTime(timezone=True), nullable=True)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    wind_kt = Column(Float, nullable=True)
    pressure_hpa = Column(Float, nullable=True)
    category = Column(String(50), nullable=True)

    storm = relationship("Storm", back_populates="track_points")


class StormTrack(Base):
    __tablename__ = "storm_tracks"

    id = Column(Integer, primary_key=True)
    storm_id = Column(UUID(as_uuid=True), ForeignKey("storms.id", ondelete="CASCADE"), nullable=False, index=True)
    track_type = Column(String(20), nullable=False)     # observed | forecast
    geojson = Column(JSONB, nullable=True)              # GeoJSON LineString
    start_time = Column(DateTime(timezone=True), nullable=True)
    end_time = Column(DateTime(timezone=True), nullable=True)

    storm = relationship("Storm", back_populates="tracks")


class StormCone(Base):
    __tablename__ = "storm_cones"

    id = Column(Integer, primary_key=True)
    storm_id = Column(UUID(as_uuid=True), ForeignKey("storms.id", ondelete="CASCADE"), nullable=False, index=True)
    cone_type = Column(String(30), nullable=False)      # forecast_cone | probability_circle
    forecast_hour = Column(Integer, nullable=True)
    geojson = Column(JSONB, nullable=True)              # GeoJSON Polygon or MultiPolygon

    storm = relationship("Storm", back_populates="cones")


class CanonicalStormLink(Base):
    __tablename__ = "canonical_storm_links"

    id = Column(Integer, primary_key=True)
    canonical_id = Column(String(50), nullable=False, index=True)
    storm_id = Column(UUID(as_uuid=True), ForeignKey("storms.id", ondelete="CASCADE"), nullable=False)
    source = Column(String(20), nullable=False)
    confidence = Column(Float, default=1.0)

    __table_args__ = (
        UniqueConstraint("canonical_id", "storm_id", name="uq_canonical_storm"),
    )


class SourceSyncLog(Base):
    __tablename__ = "source_sync_logs"

    id = Column(Integer, primary_key=True)
    source = Column(String(20), nullable=False, index=True)
    job_name = Column(String(100), nullable=False)
    status = Column(String(20), nullable=False)         # success | error | no_change | no_active
    started_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    finished_at = Column(DateTime(timezone=True), nullable=True)
    error = Column(Text, nullable=True)
    checksum = Column(String(64), nullable=True)
    raw_file_path = Column(String(500), nullable=True)
    records_processed = Column(Integer, nullable=True)
