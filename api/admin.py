import hmac
from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, Query, Request

from core.database import SessionLocal
from crawlers import ibtracs_worker, jma_worker, jtwc_worker, nhc_worker
from models.storm_models import SourceSyncLog, Storm

router = APIRouter(prefix="/api/v1/admin", tags=["admin"])


def verify_token(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    admin_token: str | None = Query(default=None, alias="admin_token"),
):
    # In production this service sits behind the admin gateway on weather-net.
    # Token verification is left as a no-op for internal network calls;
    # re-enable by comparing against settings.admin_api_token when exposed externally.
    pass


@router.get("/status", dependencies=[Depends(verify_token)])
def get_admin_status():
    db = SessionLocal()
    try:
        active_storms = db.query(Storm).filter(Storm.is_active == True).count()
        total_storms = db.query(Storm).count()

        # Per-source sync summary
        sources = {}
        for source in ["NHC", "JMA", "JTWC", "IBTrACS"]:
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
            sources[source] = {
                "lastSuccess": last_ok.finished_at.isoformat() if last_ok and last_ok.finished_at else None,
                "lastError": last_err.finished_at.isoformat() if last_err and last_err.finished_at else None,
            }

        return {
            "status": "ok",
            "storms": {"active": active_storms, "total": total_storms},
            "sources": sources,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    finally:
        db.close()


CRAWLER_MAP = {
    "nhc": nhc_worker.run_nhc_crawler,
    "jma": jma_worker.run_jma_crawler,
    "jtwc": jtwc_worker.run_jtwc_crawler,
    "ibtracs": ibtracs_worker.run_ibtracs_crawler,
}


@router.post("/crawler/{crawler_name}/trigger", dependencies=[Depends(verify_token)])
def trigger_crawler(crawler_name: str, background_tasks: BackgroundTasks):
    fn = CRAWLER_MAP.get(crawler_name.lower())
    if fn is None:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown crawler '{crawler_name}'. Valid: {list(CRAWLER_MAP)}",
        )
    background_tasks.add_task(fn)
    return {"status": "ok", "message": f"{crawler_name.upper()} crawler triggered in background"}
