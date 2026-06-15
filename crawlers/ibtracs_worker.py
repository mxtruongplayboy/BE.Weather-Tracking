"""
IBTrACS Worker - P1
Source: NOAA/NCEI International Best Track Archive for Climate Stewardship
URL: https://www.ncei.noaa.gov/products/international-best-track-archive
Polling: 1x/day or 1x/week; NOT used for realtime.

IBTrACS provides a global CSV with all historical tropical cyclone best-track data.
We import Season >= current_year - 2 and link to canonical storms where possible.
"""
import csv
import io
import logging
import zipfile
from datetime import datetime, timezone
from typing import Optional

from core.database import SessionLocal
from crawlers.base_worker import (
    fetch_with_retry,
    save_raw_file,
    sha256_of_bytes,
    sync_log,
)
from models.storm_models import CanonicalStormLink, Storm, StormTrack, StormTrackPoint
from utils.units import categorize_storm, ms_to_kt, kt_to_ms

logger = logging.getLogger(__name__)

SOURCE = "IBTrACS"

# Latest IBTrACS CSV (all basins, since 1980)
IBTRACS_CSV_URL = (
    "https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/"
    "v04r01/access/csv/ibtracs.last3years.list.v04r01.csv"
)

IBTRACS_BASIN_MAP = {
    "NA": "AL",
    "SA": "AL",
    "EP": "EP",
    "WP": "WP",
    "SP": "SP",
    "SI": "IO",
    "NI": "IO",
}


def _parse_ibtracs_time(s: str) -> Optional[datetime]:
    if not s or s.strip() == "":
        return None
    s = s.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _safe_float(s: str) -> Optional[float]:
    try:
        v = float(s.strip())
        return v if v not in (-9999, -999, 0) else None
    except (ValueError, TypeError):
        return None


def run_ibtracs_import():
    """Download and import latest IBTrACS CSV into DB."""
    with sync_log(SOURCE, "ibtracs_download_latest") as log_data:
        resp = fetch_with_retry(IBTRACS_CSV_URL, timeout=120)
        raw = resp.content
        log_data["checksum"] = sha256_of_bytes(raw)
        log_data["raw_file_path"] = save_raw_file(SOURCE, "ibtracs_last3years", "csv", raw)

        _import_csv(raw.decode("utf-8", errors="replace"), log_data)


def _import_csv(csv_text: str, log_data: dict):
    reader = csv.DictReader(io.StringIO(csv_text), skipinitialspace=True)

    # Group rows by SID (storm season ID)
    from collections import defaultdict
    storms_data: dict = defaultdict(list)

    for row in reader:
        sid = row.get("SID", "").strip()
        if not sid:
            continue
        storms_data[sid].append(row)

    log_data["records_processed"] = len(storms_data)
    logger.info(f"[IBTrACS] Found {len(storms_data)} storms in CSV")

    db = SessionLocal()
    try:
        imported = 0
        for sid, rows in storms_data.items():
            try:
                _upsert_ibtracs_storm(db, sid, rows)
                imported += 1
            except Exception as e:
                logger.warning(f"[IBTrACS] Error processing {sid}: {e}")
        db.commit()
        logger.info(f"[IBTrACS] Imported/updated {imported} storms")
    finally:
        db.close()


def _upsert_ibtracs_storm(db, sid: str, rows: list):
    if not rows:
        return

    first = rows[0]
    basin_raw = first.get("BASIN", "").strip()
    basin = IBTRACS_BASIN_MAP.get(basin_raw, basin_raw)
    name = first.get("NAME", "").strip() or "Unknown"
    season = first.get("SEASON", "").strip()

    source_id = sid

    storm = (
        db.query(Storm)
        .filter(Storm.source == SOURCE, Storm.source_storm_id == source_id)
        .first()
    )
    if storm is None:
        storm = Storm(source=SOURCE, source_storm_id=source_id)
        db.add(storm)
        db.flush()

    storm.name = name
    storm.basin = basin
    storm.is_active = False       # historical data
    storm.status = "historical"
    storm.canonical_id = f"{basin}-{season}-{sid[-4:]}"

    # Delete old track points
    db.query(StormTrackPoint).filter(
        StormTrackPoint.storm_id == storm.id,
        StormTrackPoint.point_type == "observed",
    ).delete()
    db.query(StormTrack).filter(
        StormTrack.storm_id == storm.id,
        StormTrack.track_type == "observed",
    ).delete()

    coords = []
    max_wind_kt = 0.0

    for row in rows:
        lat_s = row.get("LAT", "").strip()
        lon_s = row.get("LON", "").strip()
        lat = _safe_float(lat_s)
        lon = _safe_float(lon_s)
        if lat is None or lon is None:
            continue

        iso_time = _parse_ibtracs_time(row.get("ISO_TIME", ""))

        # Wind: IBTrACS provides WMO_WIND in knots, USA_WIND in knots
        wind_wmo = _safe_float(row.get("WMO_WIND", ""))
        wind_usa = _safe_float(row.get("USA_WIND", ""))
        wind_kt = wind_wmo or wind_usa

        pressure_wmo = _safe_float(row.get("WMO_PRES", ""))
        pressure_usa = _safe_float(row.get("USA_PRES", ""))
        pressure = pressure_wmo or pressure_usa

        if wind_kt and wind_kt > max_wind_kt:
            max_wind_kt = wind_kt

        category = categorize_storm(wind_kt, basin) if wind_kt else None
        coords.append([lon, lat])

        db.add(
            StormTrackPoint(
                storm_id=storm.id,
                point_type="observed",
                valid_time_utc=iso_time,
                lat=lat,
                lon=lon,
                wind_kt=wind_kt,
                pressure_hpa=pressure,
                category=category,
            )
        )

    if max_wind_kt > 0:
        storm.wind_kt = max_wind_kt
        storm.category = categorize_storm(max_wind_kt, basin)

    # Track the most recent observation time so "recent storms" queries work.
    valid_times = [r.get("ISO_TIME", "") for r in rows if r.get("ISO_TIME", "").strip()]
    if valid_times:
        latest_time = _parse_ibtracs_time(max(valid_times))
        if latest_time:
            storm.last_update_utc = latest_time

    if len(coords) >= 2:
        db.add(
            StormTrack(
                storm_id=storm.id,
                track_type="observed",
                geojson={"type": "LineString", "coordinates": coords},
            )
        )


def run_ibtracs_crawler():
    logger.info("[IBTrACS] Starting import")
    try:
        run_ibtracs_import()
    except Exception as e:
        logger.error(f"[IBTrACS] Import failed: {e}")
    logger.info("[IBTrACS] Done.")
