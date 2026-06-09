"""
GOES GLM (Geostationary Lightning Mapper) ingestion worker.

Data source: NOAA GOES-East (GOES-16) and GOES-West (GOES-18) GLM Level-2 LCFA
AWS S3 open bucket: s3://noaa-goes16/GLM-L2-LCFA/{YYYY}/{DOY}/{HH}/
                    s3://noaa-goes18/GLM-L2-LCFA/{YYYY}/{DOY}/{HH}/

Coverage: Americas, adjacent Pacific/Atlantic (not Vietnam/South China Sea).
Sprint 3 target.
"""
import io
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from crawlers.base_worker import fetch_with_retry, save_raw_file, sha256_of_bytes, sync_log
from core.database import SessionLocal
from core.redis import cache_set, cache_get
from models.lightning_models import LightningEvent

logger = logging.getLogger(__name__)

SOURCE_GOES_EAST = "GOES_GLM"
GOES_EAST_BUCKET = "noaa-goes16"
GOES_WEST_BUCKET = "noaa-goes18"
GLM_PREFIX = "GLM-L2-LCFA"

# S3 HTTP endpoint (no-auth, public)
S3_BASE = "https://{bucket}.s3.amazonaws.com"

# GOES coverage bounding box (approximate)
GOES_COVERAGE_NOTE = (
    "Real lightning strike data available for Americas and adjacent oceans via NOAA GOES GLM. "
    "Coverage outside this region uses Lightning Risk forecast instead."
)


def _goes_s3_url(bucket: str, year: int, doy: int, hour: int, filename: str) -> str:
    base = S3_BASE.format(bucket=bucket)
    return f"{base}/{GLM_PREFIX}/{year}/{doy:03d}/{hour:02d}/{filename}"


def _list_glm_files(bucket: str, year: int, doy: int, hour: int) -> list[str]:
    """List GLM NetCDF files for a given hour via S3 XML listing."""
    base = S3_BASE.format(bucket=bucket)
    prefix = f"{GLM_PREFIX}/{year}/{doy:03d}/{hour:02d}/"
    url = f"{base}?list-type=2&prefix={prefix}&max-keys=20"
    try:
        resp = fetch_with_retry(url, timeout=15)
        # Parse XML listing
        import xml.etree.ElementTree as ET
        ns = "http://s3.amazonaws.com/doc/2006-03-01/"
        root = ET.fromstring(resp.text)
        keys = [el.text for el in root.findall(f"{{{ns}}}Contents/{{{ns}}}Key")]
        return [k.split("/")[-1] for k in keys if k and k.endswith(".nc")]
    except Exception as e:
        logger.warning(f"[GOES GLM] Failed to list S3 files for {bucket}/{prefix}: {e}")
        return []


def _parse_glm_netcdf(data: bytes, satellite: str, file_time: datetime) -> list[dict]:
    """Parse GLM Level-2 LCFA NetCDF4 file, return list of flash event dicts."""
    try:
        import netCDF4 as nc
        import numpy as np
        ds = nc.Dataset("inmemory.nc", memory=data)

        events = []
        try:
            flash_lats = ds.variables["flash_lat"][:]
            flash_lons = ds.variables["flash_lon"][:]

            # Time: seconds since epoch stored as flash_time_offset_of_first_constituent_event
            # relative to product_time (seconds since 2000-01-01 12:00:00)
            J2000_EPOCH = datetime(2000, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

            time_offsets = None
            if "flash_time_offset_of_first_constituent_event" in ds.variables:
                time_offsets = ds.variables["flash_time_offset_of_first_constituent_event"][:]

            energies = ds.variables.get("flash_energy")
            areas = ds.variables.get("flash_area")
            flash_ids = ds.variables.get("flash_id")

            prod_time_var = ds.variables.get("product_time")
            prod_time_secs = float(prod_time_var[:]) if prod_time_var is not None else None

            n = len(flash_lats)
            for i in range(n):
                lat = float(flash_lats[i])
                lon = float(flash_lons[i])
                if np.ma.is_masked(lat) or np.ma.is_masked(lon):
                    continue

                # Compute flash time
                flash_time = file_time
                if prod_time_secs is not None and time_offsets is not None:
                    total_secs = prod_time_secs + float(time_offsets[i])
                    flash_time = J2000_EPOCH + timedelta(seconds=total_secs)

                fid = int(flash_ids[i]) if flash_ids is not None else i
                source_event_id = f"{satellite}_{file_time.strftime('%Y%m%dT%H%M%SZ')}_{fid}"

                events.append({
                    "source_event_id": source_event_id,
                    "lat": lat,
                    "lon": lon,
                    "time_utc": flash_time,
                    "energy": float(energies[i]) if energies is not None and not np.ma.is_masked(energies[i]) else None,
                    "area_km2": float(areas[i]) / 1e6 if areas is not None and not np.ma.is_masked(areas[i]) else None,
                })
        finally:
            ds.close()

        return events
    except ImportError:
        logger.error("[GOES GLM] netCDF4 not installed — cannot parse GLM files. Run: pip install netCDF4")
        return []
    except Exception as e:
        logger.error(f"[GOES GLM] NetCDF parse error: {e}")
        return []


def _upsert_events(events: list[dict], satellite: str, product: str, db) -> int:
    """Upsert lightning events into DB, skip existing source_event_id."""
    inserted = 0
    for ev in events:
        existing = db.query(LightningEvent).filter(
            LightningEvent.source == SOURCE_GOES_EAST,
            LightningEvent.source_event_id == ev["source_event_id"],
        ).first()
        if existing:
            continue

        db.add(LightningEvent(
            source=SOURCE_GOES_EAST,
            source_event_id=ev["source_event_id"],
            satellite=satellite,
            product=product,
            event_type="flash",
            time_utc=ev["time_utc"],
            lat=ev["lat"],
            lon=ev["lon"],
            energy=ev.get("energy"),
            area_km2=ev.get("area_km2"),
        ))
        inserted += 1

    if inserted:
        db.commit()
    return inserted


def _fetch_and_ingest_hour(bucket: str, satellite: str, year: int, doy: int, hour: int):
    """Fetch the latest GLM file for a given hour and ingest events."""
    files = _list_glm_files(bucket, year, doy, hour)
    if not files:
        return 0

    # Take the most recent file
    filename = sorted(files)[-1]
    url = _goes_s3_url(bucket, year, doy, hour, filename)

    with sync_log(SOURCE_GOES_EAST, "goes_glm_fetch_hour") as log_data:
        resp = fetch_with_retry(url, timeout=60)
        raw = resp.content
        log_data["checksum"] = sha256_of_bytes(raw)
        log_data["raw_file_path"] = save_raw_file(SOURCE_GOES_EAST, "goes_glm", "nc", raw)

        # Parse file time from filename: OR_GLM-L2-LCFA_G16_s20261611900...
        try:
            parts = filename.split("_")
            time_str = [p for p in parts if p.startswith("s")][0][1:14]  # sYYYYDDDHHMMS
            file_time = datetime.strptime(time_str, "%Y%j%H%M%S").replace(tzinfo=timezone.utc)
        except Exception:
            file_time = datetime.now(timezone.utc)

        events = _parse_glm_netcdf(raw, satellite, file_time)

        db = SessionLocal()
        try:
            inserted = _upsert_events(events, satellite, "GLM-L2-LCFA", db)
            log_data["records_processed"] = inserted
            return inserted
        finally:
            db.close()


def run_goes_glm_crawler():
    """
    Discover and ingest the most recent GLM file from GOES-East (16) and GOES-West (18).
    Runs every 1-5 minutes via scheduler.
    """
    logger.info("[GOES GLM] Starting crawler")
    now = datetime.now(timezone.utc)
    year = now.year
    doy = now.timetuple().tm_yday
    hour = now.hour

    total = 0
    for bucket, satellite in [(GOES_EAST_BUCKET, "GOES_EAST"), (GOES_WEST_BUCKET, "GOES_WEST")]:
        try:
            n = _fetch_and_ingest_hour(bucket, satellite, year, doy, hour)
            total += n
            logger.info(f"[GOES GLM] {satellite}: {n} new flash events")
        except Exception as e:
            logger.error(f"[GOES GLM] {satellite} ingestion failed: {e}")

    # Invalidate recent cache
    from core.redis import cache_delete
    cache_delete("lightning:events:recent:GOES_GLM:15")
    cache_delete("lightning:events:recent:GOES_GLM:30")
    cache_delete("lightning:events:recent:GOES_GLM:60")

    logger.info(f"[GOES GLM] Done. Total new events: {total}")


def _delete_old_events(retention_days: int = 30):
    """Purge lightning events older than retention_days. Run as daily maintenance job."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    db = SessionLocal()
    try:
        deleted = db.query(LightningEvent).filter(LightningEvent.time_utc < cutoff).delete()
        db.commit()
        logger.info(f"[GOES GLM] Purged {deleted} old events (older than {retention_days} days)")
    finally:
        db.close()
