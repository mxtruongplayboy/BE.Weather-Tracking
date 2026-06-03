from fastapi import APIRouter, Depends, HTTPException, Header, Query, BackgroundTasks, Request
import os
import hmac
from datetime import datetime

from core.database import SessionLocal
from models.storm import Storm
from crawlers import nhc_worker, jtwc_worker

router = APIRouter(prefix="/api/v1/admin", tags=["admin"])

def verify_token(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    admin_token: str | None = Query(default=None, alias="admin_token"),
):
    expected = os.environ.get("ADMIN_API_TOKEN", "change_me_in_production").strip()
    provided = (x_admin_token or admin_token or "").strip()
    
    if not provided or not hmac.compare_digest(provided, expected):
        print(f"DEBUG: Token mismatch! Received='{provided}' Expected='{expected}'")
        raise HTTPException(status_code=401, detail="Invalid or missing X-Admin-Token")

@router.get("/status", dependencies=[Depends(verify_token)])
def get_admin_status():
    db = SessionLocal()
    try:
        active_storms = db.query(Storm).filter(Storm.is_active == True).count()
        total_storms = db.query(Storm).count()
        return {
            "status": "ok",
            "storms": {
                "active": active_storms,
                "total": total_storms
            },
            # TODO: Track crawler runs if needed, using a simple global var or db
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
    finally:
        db.close()

@router.post("/crawler/{crawler_name}/trigger", dependencies=[Depends(verify_token)])
def trigger_crawler(crawler_name: str, background_tasks: BackgroundTasks):
    if crawler_name == "nhc":
        background_tasks.add_task(nhc_worker.run_nhc_crawler)
        return {"status": "ok", "message": "NHC Crawler triggered in background"}
    elif crawler_name == "jtwc":
        background_tasks.add_task(jtwc_worker.run_jtwc_crawler)
        return {"status": "ok", "message": "JTWC Crawler triggered in background"}
    else:
        raise HTTPException(status_code=400, detail="Unknown crawler name")
