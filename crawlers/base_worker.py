"""
Base worker with shared retry/backoff, raw-file storage, checksum, and sync logging.
"""
import hashlib
import logging
import os
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from core.config import settings
from core.database import SessionLocal
from models.storm_models import SourceSyncLog

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (compatible; WeatherTrackingBot/1.0; "
                "+https://github.com/weather-tracking)"
            ),
            "Accept": "application/json, text/html, */*",
        }
    )
    return s


HTTP_SESSION = _make_session()


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    stop=stop_after_attempt(4),
    reraise=True,
)
def fetch_with_retry(
    url: str,
    timeout: int = 20,
    extra_headers: Optional[dict] = None,
    stream: bool = False,
) -> requests.Response:
    headers = extra_headers or {}
    resp = HTTP_SESSION.get(url, headers=headers, timeout=timeout, stream=stream)
    resp.raise_for_status()
    return resp


def sha256_of_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def save_raw_file(source: str, job_name: str, ext: str, data: bytes) -> str:
    """Persist raw fetched bytes to disk, return the file path."""
    directory = os.path.join(settings.raw_data_dir, source)
    os.makedirs(directory, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filename = f"{job_name}_{ts}.{ext}"
    path = os.path.join(directory, filename)
    with open(path, "wb") as f:
        f.write(data)
    return path


@contextmanager
def sync_log(source: str, job_name: str):
    """
    Context manager that writes a SourceSyncLog entry.
    Yields a mutable dict so the caller can set checksum/raw_file_path/records_processed.
    On success sets status='success'; on exception sets status='error' and stores message.
    """
    log_data = {
        "checksum": None,
        "raw_file_path": None,
        "records_processed": None,
    }
    db = SessionLocal()
    log_entry = SourceSyncLog(
        source=source,
        job_name=job_name,
        status="running",
        started_at=_utcnow(),
    )
    db.add(log_entry)
    db.commit()
    db.refresh(log_entry)

    try:
        yield log_data
        log_entry.status = "success"
    except Exception as exc:
        log_entry.status = "error"
        log_entry.error = str(exc)
        logger.error(f"[{source}/{job_name}] {exc}")
        raise
    finally:
        log_entry.finished_at = _utcnow()
        log_entry.checksum = log_data.get("checksum")
        log_entry.raw_file_path = log_data.get("raw_file_path")
        log_entry.records_processed = log_data.get("records_processed")
        try:
            db.commit()
        except Exception:
            pass
        db.close()


def get_last_sync(source: str, job_name: str) -> Optional[SourceSyncLog]:
    db = SessionLocal()
    try:
        return (
            db.query(SourceSyncLog)
            .filter(
                SourceSyncLog.source == source,
                SourceSyncLog.job_name == job_name,
                SourceSyncLog.status == "success",
            )
            .order_by(SourceSyncLog.finished_at.desc())
            .first()
        )
    finally:
        db.close()
