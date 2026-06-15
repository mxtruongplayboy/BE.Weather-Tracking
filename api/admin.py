import hmac
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, Query, Request
from sqlalchemy import func

from core.database import SessionLocal
from crawlers import ibtracs_worker, jma_worker, jtwc_worker, nhc_worker
from crawlers import goes_glm_worker, gfs_risk_worker, mtg_li_worker, nasa_lis_worker
from models.storm_models import SourceSyncLog, Storm
from models.lightning_models import LightningEvent, LightningRiskForecast, LightningSource

router = APIRouter(prefix="/api/v1/admin", tags=["admin"])


def verify_token(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    admin_token: str | None = Query(default=None, alias="admin_token"),
):
    pass


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _source_health(db, source: str) -> dict:
    """Return last success/error info for a source code."""
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

    last_success_dt = last_ok.finished_at if last_ok and last_ok.finished_at else None
    stale_minutes = None
    is_stale = True
    if last_success_dt:
        dt = last_success_dt.replace(tzinfo=timezone.utc) if last_success_dt.tzinfo is None else last_success_dt
        delta = _utcnow() - dt
        stale_minutes = int(delta.total_seconds() / 60)
        is_stale = stale_minutes > 90

    return {
        "lastSuccess": last_success_dt.isoformat() if last_success_dt else None,
        "lastError": last_err.finished_at.isoformat() if last_err and last_err.finished_at else None,
        "lastErrorMessage": last_err.error if last_err else None,
        "isStale": is_stale,
        "staleMinutes": stale_minutes,
        "recordsLastRun": last_ok.records_processed if last_ok else None,
    }


# ── /status ───────────────────────────────────────────────────────────────────

@router.get("/status", dependencies=[Depends(verify_token)])
def get_admin_status():
    """Full system health: storms + lightning sources + data counts."""
    db = SessionLocal()
    try:
        now = _utcnow()

        # ── Storm stats ───────────────────────────────────────────────────────
        active_storms = db.query(Storm).filter(Storm.is_active == True).count()
        total_storms = db.query(Storm).count()

        storm_sources = {}
        for source in ["NHC", "JMA", "JTWC", "IBTrACS"]:
            storm_sources[source] = _source_health(db, source)

        # ── Lightning event stats ─────────────────────────────────────────────
        total_events = db.query(LightningEvent).count()
        events_last_hour = db.query(LightningEvent).filter(
            LightningEvent.time_utc >= now - timedelta(hours=1)
        ).count()
        events_last_24h = db.query(LightningEvent).filter(
            LightningEvent.time_utc >= now - timedelta(hours=24)
        ).count()

        # Events breakdown by source
        event_by_source = {}
        rows = (
            db.query(LightningEvent.source, func.count(LightningEvent.id))
            .filter(LightningEvent.time_utc >= now - timedelta(hours=24))
            .group_by(LightningEvent.source)
            .all()
        )
        for src, cnt in rows:
            event_by_source[src] = cnt

        # ── Lightning risk stats ──────────────────────────────────────────────
        latest_gfs_run = (
            db.query(LightningRiskForecast.run_time_utc)
            .filter(LightningRiskForecast.model_source == "GFS")
            .order_by(LightningRiskForecast.run_time_utc.desc())
            .first()
        )
        gfs_grid_count = db.query(LightningRiskForecast).filter(
            LightningRiskForecast.model_source == "GFS"
        ).count() if latest_gfs_run else 0

        risk_by_level = {}
        if latest_gfs_run:
            rows = (
                db.query(LightningRiskForecast.risk_level, func.count(LightningRiskForecast.id))
                .filter(
                    LightningRiskForecast.model_source == "GFS",
                    LightningRiskForecast.run_time_utc == latest_gfs_run[0],
                    LightningRiskForecast.forecast_hour == 0,
                )
                .group_by(LightningRiskForecast.risk_level)
                .all()
            )
            for lvl, cnt in rows:
                risk_by_level[lvl] = cnt

        # ── Lightning source health ───────────────────────────────────────────
        lightning_sources = {}
        for source in ["GOES_GLM", "MTG_LI", "GFS_RISK", "NASA_LIS"]:
            lightning_sources[source] = _source_health(db, source)

        return {
            "status": "ok",
            "timestamp": now.isoformat(),
            "storms": {
                "active": active_storms,
                "total": total_storms,
                "sources": storm_sources,
            },
            "lightning": {
                "events": {
                    "total": total_events,
                    "last1h": events_last_hour,
                    "last24h": events_last_24h,
                    "bySource": event_by_source,
                },
                "riskGrid": {
                    "model": "GFS",
                    "latestRun": latest_gfs_run[0].isoformat() if latest_gfs_run else None,
                    "totalGridPoints": gfs_grid_count,
                    "currentHourByLevel": risk_by_level,
                },
                "sources": lightning_sources,
            },
        }
    finally:
        db.close()


# ── /lightning/events ─────────────────────────────────────────────────────────

@router.get("/lightning/events", dependencies=[Depends(verify_token)])
def list_lightning_events(
    source: Optional[str] = Query(None, description="GOES_GLM | MTG_LI"),
    since_hours: int = Query(1, ge=1, le=72),
    limit: int = Query(100, ge=1, le=1000),
):
    """List recent lightning events. Useful for debug and dashboard."""
    db = SessionLocal()
    try:
        since = _utcnow() - timedelta(hours=since_hours)
        q = db.query(LightningEvent).filter(LightningEvent.time_utc >= since)
        if source:
            q = q.filter(LightningEvent.source == source.upper())
        events = q.order_by(LightningEvent.time_utc.desc()).limit(limit).all()

        return {
            "count": len(events),
            "sinceHours": since_hours,
            "events": [
                {
                    "id": str(ev.id),
                    "source": ev.source,
                    "satellite": ev.satellite,
                    "eventType": ev.event_type,
                    "timeUtc": ev.time_utc.isoformat(),
                    "lat": ev.lat,
                    "lon": ev.lon,
                    "energy": ev.energy,
                    "areKm2": ev.area_km2,
                }
                for ev in events
            ],
        }
    finally:
        db.close()


# ── /lightning/risk ───────────────────────────────────────────────────────────

@router.get("/lightning/risk", dependencies=[Depends(verify_token)])
def list_risk_grid(
    model: str = Query("GFS"),
    forecast_hour: int = Query(0),
    min_risk: float = Query(0.5, description="Filter by minimum risk_score"),
    limit: int = Query(200, ge=1, le=2000),
):
    """List high-risk grid points from latest GFS run."""
    db = SessionLocal()
    try:
        latest_run = (
            db.query(LightningRiskForecast.run_time_utc)
            .filter(LightningRiskForecast.model_source == model.upper())
            .order_by(LightningRiskForecast.run_time_utc.desc())
            .first()
        )
        if not latest_run:
            return {"runTime": None, "count": 0, "points": []}

        rows = (
            db.query(LightningRiskForecast)
            .filter(
                LightningRiskForecast.model_source == model.upper(),
                LightningRiskForecast.run_time_utc == latest_run[0],
                LightningRiskForecast.forecast_hour == forecast_hour,
                LightningRiskForecast.risk_score >= min_risk,
            )
            .order_by(LightningRiskForecast.risk_score.desc())
            .limit(limit)
            .all()
        )

        return {
            "model": model.upper(),
            "runTime": latest_run[0].isoformat(),
            "forecastHour": forecast_hour,
            "minRisk": min_risk,
            "count": len(rows),
            "points": [
                {
                    "lat": r.lat,
                    "lon": r.lon,
                    "riskScore": r.risk_score,
                    "riskLevel": r.risk_level,
                    "validTime": r.valid_time_utc.isoformat(),
                    "cape_jkg": r.cape_jkg,
                    "convective_precip_mm": r.convective_precip_mm,
                }
                for r in rows
            ],
        }
    finally:
        db.close()


# ── /lightning/sources ────────────────────────────────────────────────────────

@router.get("/lightning/sources", dependencies=[Depends(verify_token)])
def list_lightning_sources():
    """Return lightning source registry with enabled/disabled status."""
    db = SessionLocal()
    try:
        sources = db.query(LightningSource).all()
        return {
            "sources": [
                {
                    "code": s.code,
                    "name": s.name,
                    "sourceType": s.source_type,
                    "coverageNote": s.coverage_note,
                    "enabled": s.enabled,
                    "createdAt": s.created_at.isoformat() if s.created_at else None,
                }
                for s in sources
            ]
        }
    finally:
        db.close()


# ── Crawler trigger ───────────────────────────────────────────────────────────

CRAWLER_MAP = {
    # Storm crawlers
    "nhc": nhc_worker.run_nhc_crawler,
    "jma": jma_worker.run_jma_crawler,
    "jtwc": jtwc_worker.run_jtwc_crawler,
    "ibtracs": ibtracs_worker.run_ibtracs_crawler,
    # Lightning crawlers
    "goes_glm": goes_glm_worker.run_goes_glm_crawler,
    "gfs_risk": gfs_risk_worker.run_gfs_risk_crawler,
    "mtg_li": mtg_li_worker.run_mtg_li_crawler,
    # nasa_lis uses the safe wrapper: network probe + exception guard + concurrency lock
    "nasa_lis": nasa_lis_worker.safe_run_nasa_lis_import,
}


@router.post("/crawler/{crawler_name}/trigger", dependencies=[Depends(verify_token)])
def trigger_crawler(crawler_name: str, background_tasks: BackgroundTasks):
    """Manually trigger any crawler by name."""
    fn = CRAWLER_MAP.get(crawler_name.lower())
    if fn is None:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown crawler '{crawler_name}'. Valid: {sorted(CRAWLER_MAP)}",
        )

    # For nasa_lis, reject immediately if already in progress so admin UI gets honest feedback.
    if crawler_name.lower() == "nasa_lis" and nasa_lis_worker.is_nasa_lis_running():
        return {
            "status": "skipped",
            "message": "nasa_lis import is already running — please wait for it to finish",
        }

    background_tasks.add_task(fn)
    return {"status": "ok", "message": f"'{crawler_name}' crawler triggered in background"}


# ── Sync logs ─────────────────────────────────────────────────────────────────

@router.get("/sync-logs", dependencies=[Depends(verify_token)])
def get_sync_logs(
    source: Optional[str] = Query(None),
    status: Optional[str] = Query(None, description="success | error | running"),
    limit: int = Query(50, ge=1, le=500),
):
    """Return recent source_sync_logs for all storm + lightning sources."""
    db = SessionLocal()
    try:
        q = db.query(SourceSyncLog)
        if source:
            q = q.filter(SourceSyncLog.source == source.upper())
        if status:
            q = q.filter(SourceSyncLog.status == status)
        logs = q.order_by(SourceSyncLog.started_at.desc()).limit(limit).all()

        return {
            "count": len(logs),
            "logs": [
                {
                    "id": log.id,
                    "source": log.source,
                    "jobName": log.job_name,
                    "status": log.status,
                    "startedAt": log.started_at.isoformat() if log.started_at else None,
                    "finishedAt": log.finished_at.isoformat() if log.finished_at else None,
                    "recordsProcessed": log.records_processed,
                    "error": log.error,
                    "rawFilePath": log.raw_file_path,
                }
                for log in logs
            ],
        }
    finally:
        db.close()
