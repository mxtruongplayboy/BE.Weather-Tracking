"""
Disk/DB housekeeping. Every crawler run writes a raw payload file and a
source_sync_logs row; neither was ever deleted, so the raw_data volume and the
Postgres volume grew until the VPS disk hit 100%.
"""
import logging
import os
import time
from datetime import datetime, timedelta, timezone

from core.config import settings
from core.database import SessionLocal
from models.storm_models import SourceSyncLog

logger = logging.getLogger(__name__)


def purge_raw_files(retention_days: int | None = None) -> int:
    """Delete raw payload files older than retention_days. Returns files removed."""
    days = settings.raw_retention_days if retention_days is None else retention_days
    cutoff = time.time() - days * 86400
    removed = 0
    freed = 0
    for root, _dirs, files in os.walk(settings.raw_data_dir):
        for name in files:
            path = os.path.join(root, name)
            try:
                st = os.stat(path)
                if st.st_mtime < cutoff:
                    os.remove(path)
                    removed += 1
                    freed += st.st_size
            except OSError as e:
                logger.warning(f"[MAINT] Could not remove {path}: {e}")
    logger.info(f"[MAINT] Raw files purged: {removed} ({freed / 1e9:.2f} GB, older than {days}d)")
    return removed


def purge_sync_logs(retention_days: int | None = None) -> int:
    """Delete source_sync_logs rows older than retention_days."""
    days = settings.sync_log_retention_days if retention_days is None else retention_days
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    db = SessionLocal()
    try:
        deleted = (
            db.query(SourceSyncLog)
            .filter(SourceSyncLog.started_at < cutoff)
            .delete(synchronize_session=False)
        )
        db.commit()
        logger.info(f"[MAINT] Sync logs purged: {deleted} (older than {days}d)")
        return deleted
    finally:
        db.close()


def run_maintenance():
    """Daily job: purge raw files, sync logs and stale GFS risk runs."""
    from crawlers.gfs_risk_worker import purge_old_risk_runs

    for step in (purge_raw_files, purge_sync_logs, purge_old_risk_runs):
        try:
            step()
        except Exception as e:
            logger.error(f"[MAINT] {step.__name__} failed: {e}")
