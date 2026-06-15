"""
NASA LIS/OTD Historical Lightning Importer — Sprint 5.

Purpose: historical climatology, training data, risk model validation.
NOT used as a realtime source.

Data: NASA GHRC DAAC LIS on TRMM / ISS
URL: https://ghrc.nsstc.nasa.gov/lightning/data/data_lis_trmm/LISOTD_HRMC/
Product: LISOTD_HRMC (High Resolution Monthly Climatology) — HDF5 grid file

The monthly climatology gives flash rate (fl/km²/day) per 0.5° grid cell.
We use this as a prior probability boost for risk scoring.

Run as a batch job: weekly or monthly (not realtime).
"""
import logging
import os
import threading
from datetime import datetime, timezone
from typing import Optional

from crawlers.base_worker import fetch_with_retry, save_raw_file, sha256_of_bytes, sync_log
from core.database import SessionLocal

logger = logging.getLogger(__name__)

# Prevent concurrent runs (manual trigger + cron overlap)
_NASA_LIS_LOCK = threading.Lock()
_nasa_lis_running = False

SOURCE = "NASA_LIS"

# NASA GHRC DAAC — LIS/OTD High Resolution Monthly Climatology
NASA_BASE = "https://ghrc.nsstc.nasa.gov/lightning/data/data_lis_trmm/LISOTD_HRMC/"
# Filename pattern: LISOTD_HRMC_V2.3.2015.hdf or similar
# We use the climatology grid indexed by month (1-12).

NASA_LIS_COVERAGE_NOTE = (
    "NASA LIS/OTD historical lightning climatology used for risk model validation. "
    "Not a realtime source."
)


def _discover_hrmc_file() -> Optional[str]:
    """
    Return URL of the HDF5 monthly climatology file.
    Falls back to a known stable path if listing fails.
    """
    # Known stable file from public GHRC archive
    return f"{NASA_BASE}data/LISOTD_HRMC_V2.3.2014.hdf"


def _parse_hrmc_hdf5(data: bytes) -> list[dict]:
    """
    Parse NASA LIS/OTD HRMC HDF5 climatology file.
    Returns list of {lat, lon, month, flash_rate_fl_km2_day}.
    """
    try:
        import h5py
        import numpy as np
        import io

        with h5py.File(io.BytesIO(data), "r") as f:
            # HRMC structure: 12 groups, one per month
            # Each has "Climatology/HRMC_COM" dataset (720×1440 for 0.25°)
            results = []

            for month in range(1, 13):
                key = f"Climatology/HRMC_COM"
                if key not in f:
                    # Try alternate structure
                    month_key = f"{month:02d}"
                    if month_key in f:
                        data_arr = f[month_key].get(key)
                    else:
                        continue
                else:
                    data_arr = f[key]

                if data_arr is None:
                    continue

                arr = np.array(data_arr)
                n_lat, n_lon = arr.shape
                lat_step = 180.0 / n_lat
                lon_step = 360.0 / n_lon

                for i in range(n_lat):
                    for j in range(n_lon):
                        val = float(arr[i, j])
                        if val <= 0 or np.isnan(val):
                            continue
                        lat = 90.0 - (i + 0.5) * lat_step
                        lon = -180.0 + (j + 0.5) * lon_step
                        results.append({
                            "lat": lat,
                            "lon": lon,
                            "month": month,
                            "flash_rate": val,
                        })

            return results

    except ImportError:
        logger.error("[NASA LIS] h5py not installed. Run: pip install h5py")
        return []
    except Exception as e:
        logger.error(f"[NASA LIS] HDF5 parse error: {e}")
        return []


def _store_climatology(points: list[dict], db) -> int:
    """
    Store monthly climatology data in a simple JSON column on LightningRiskForecast
    with model_source='NASA_LIS_CLIM'. Used as prior for risk scoring.

    For MVP, we just log the count since a full climatology table is separate.
    In production, add a lightning_climatology table.
    """
    # For MVP: store as a special model_source record per month/location
    from models.lightning_models import LightningRiskForecast
    from datetime import date

    now = datetime.now(timezone.utc)
    run_dt = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    inserted = 0
    for pt in points:
        # Use month-start as valid_time
        valid_dt = now.replace(month=pt["month"], day=1, hour=0, minute=0, second=0, microsecond=0)
        flash_rate = pt["flash_rate"]

        # Normalize to 0..1 risk prior (flash rate 0..10 fl/km²/day → score)
        prior = min(1.0, flash_rate / 10.0)
        risk_level = "very_high" if prior >= 0.75 else "high" if prior >= 0.5 else "moderate" if prior >= 0.25 else "low"

        existing = db.query(LightningRiskForecast).filter(
            LightningRiskForecast.model_source == "NASA_LIS_CLIM",
            LightningRiskForecast.valid_time_utc == valid_dt,
            LightningRiskForecast.lat.between(pt["lat"] - 0.1, pt["lat"] + 0.1),
            LightningRiskForecast.lon.between(pt["lon"] - 0.1, pt["lon"] + 0.1),
        ).first()

        if existing:
            continue

        db.add(LightningRiskForecast(
            model_source="NASA_LIS_CLIM",
            run_time_utc=run_dt,
            valid_time_utc=valid_dt,
            forecast_hour=0,
            resolution_deg=0.25,
            lat=pt["lat"],
            lon=pt["lon"],
            risk_score=prior,
            risk_level=risk_level,
            raw={"flash_rate_fl_km2_day": flash_rate, "month": pt["month"]},
        ))
        inserted += 1

        # Commit in batches to avoid memory pressure
        if inserted % 10000 == 0:
            db.commit()
            logger.info(f"[NASA LIS] Stored {inserted} climatology points so far…")

    db.commit()
    return inserted


def _check_network_reachable() -> bool:
    """Quick connectivity probe to GHRC host before spending retries on it."""
    import socket
    try:
        socket.setdefaulttimeout(5)
        socket.create_connection(("ghrc.nsstc.nasa.gov", 443), timeout=5).close()
        return True
    except OSError:
        return False


def run_nasa_lis_import():
    """
    Batch import NASA LIS/OTD monthly climatology.
    Intended to run weekly or monthly; NOT a realtime job.
    """
    logger.info("[NASA LIS] Starting historical climatology import")

    url = _discover_hrmc_file()
    if url is None:
        logger.error("[NASA LIS] No HDF5 file URL found")
        return

    with sync_log(SOURCE, "nasa_lis_hrmc_import") as log_data:
        resp = fetch_with_retry(url, timeout=300)
        raw = resp.content
        log_data["checksum"] = sha256_of_bytes(raw)
        log_data["raw_file_path"] = save_raw_file(SOURCE, "nasa_lis_hrmc", "hdf", raw)

        points = _parse_hrmc_hdf5(raw)
        if not points:
            log_data["records_processed"] = 0
            logger.warning("[NASA LIS] No climatology points parsed")
            return

        db = SessionLocal()
        try:
            n = _store_climatology(points, db)
            log_data["records_processed"] = n
            logger.info(f"[NASA LIS] Import complete. Stored {n} climatology records")
        finally:
            db.close()


def safe_run_nasa_lis_import() -> dict:
    """
    Safe wrapper for run_nasa_lis_import used by scheduler and admin trigger.
    - Prevents concurrent executions via a module-level lock.
    - Does a quick network probe first to avoid wasting 4 retries × 300 s timeout.
    - Catches all exceptions so ASGI background tasks and APScheduler jobs
      never surface an unhandled exception to the server process.
    Returns a status dict so callers can inspect the outcome.
    """
    global _nasa_lis_running

    with _NASA_LIS_LOCK:
        if _nasa_lis_running:
            logger.info("[NASA LIS] Already running — skipping duplicate invocation")
            return {"status": "skipped", "reason": "already_running"}
        _nasa_lis_running = True

    try:
        if not _check_network_reachable():
            logger.warning(
                "[NASA LIS] Network unreachable (ghrc.nsstc.nasa.gov:443). "
                "Skipping import — check container outbound connectivity."
            )
            return {"status": "skipped", "reason": "network_unreachable"}

        run_nasa_lis_import()
        return {"status": "ok"}
    except Exception:
        logger.exception("[NASA LIS] Import failed — caught by safe wrapper")
        return {"status": "error"}
    finally:
        with _NASA_LIS_LOCK:
            _nasa_lis_running = False


def is_nasa_lis_running() -> bool:
    return _nasa_lis_running
