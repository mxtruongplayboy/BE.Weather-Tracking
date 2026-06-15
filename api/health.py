"""
Health endpoints — show per-source lastSuccess/lastError and staleness.
Polled every 5 minutes by monitoring.
"""
from datetime import datetime, timezone
from typing import List

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from core.config import settings
from core.database import get_db
from models.schemas import HealthResponse, SourceHealthEntry
from models.storm_models import SourceSyncLog, Storm

router = APIRouter(prefix="/api/v1", tags=["health"])

SOURCES = ["NHC", "JMA", "JTWC", "IBTrACS"]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _stale_minutes(dt: datetime) -> int:
    delta = _utcnow() - dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else _utcnow() - dt
    return int(delta.total_seconds() / 60)


@router.get("/health", response_model=HealthResponse)
def health(db: Session = Depends(get_db)):
    entries: List[SourceHealthEntry] = []

    for source in SOURCES:
        last_ok = (
            db.query(SourceSyncLog)
            .filter(SourceSyncLog.source == source, SourceSyncLog.status == "success")
            .order_by(SourceSyncLog.finished_at.desc())
            .first()
        )
        last_err = (
            db.query(SourceSyncLog)
            .filter(SourceSyncLog.source == source, SourceSyncLog.status == "error")
            .order_by(SourceSyncLog.finished_at.desc())
            .first()
        )

        stale = False
        stale_min = None
        if last_ok and last_ok.finished_at:
            stale_min = _stale_minutes(last_ok.finished_at)
            stale = stale_min > settings.stale_threshold_minutes

        entries.append(
            SourceHealthEntry(
                source=source,
                lastSuccess=last_ok.finished_at if last_ok else None,
                lastError=last_err.finished_at if last_err else None,
                lastErrorMessage=last_err.error if last_err else None,
                isStale=stale,
                staleMinutes=stale_min if stale else None,
            )
        )

    overall = "ok" if not any(e.isStale for e in entries) else "degraded"

    return HealthResponse(
        status=overall,
        sources=entries,
        checkedAt=_utcnow(),
    )


@router.get("/health/simple")
def health_simple():
    return {"status": "ok", "service": "weather-tracking"}


# Alias for Docker/Nginx/LB health checks that probe /health instead of /api/v1/health
alias_router = APIRouter(tags=["health"])


@alias_router.get("/health", include_in_schema=False)
def health_root_alias():
    return {"status": "ok", "service": "weather-tracking"}
