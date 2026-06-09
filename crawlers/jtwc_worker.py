"""
JTWC Worker - P1
Covers: West Pacific (WP), Indian Ocean (IO), South Pacific (SP)
Source: https://www.metoc.dc3n.navy.mil/jtwc/products/
Polling: active warnings 30 min, best track 1x/day

JTWC publishes text advisories in the ATCF format and also provides
a products directory listing active storms.

ATCF deck format (A-deck / B-deck columns, space-separated):
  BASIN, CY, YYYYMMDDHH, TECHNUM, TECH, TAU, LAT, LON, VMAX, MSLP, TY, ...

This parser reads the current warnings index and individual storm files.
"""
import logging
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from core.database import SessionLocal
from core.redis import cache_invalidate_storms
from crawlers.base_worker import (
    HTTP_SESSION,
    save_raw_file,
    sha256_of_bytes,
    sync_log,
)
from models.storm_models import Storm, StormTrack, StormTrackPoint
from utils.geo import compute_movement
from utils.units import categorize_storm

logger = logging.getLogger(__name__)

SOURCE = "JTWC"

NHC_ATCF_BTK_URL = "https://ftp.nhc.noaa.gov/atcf/btk/"

JTWC_BASIN_PREFIXES = {"WP": "wp", "IO": "io", "SH": "sh", "SP": "sh"}

ATCF_TY_CATEGORIES = {
    "TD": "tropical_depression",
    "TS": "tropical_storm",
    "TY": "typhoon",
    "ST": "violent_typhoon",
    "TC": "typhoon",
    "HU": "category_1",
    "SD": "tropical_depression",
    "SS": "tropical_storm",
    "EX": "extratropical",
    "LO": "low",
    "WV": "tropical_wave",
    "ET": "extratropical",
    "XX": "unknown",
}


def _parse_atcf_lat(s: str) -> Optional[float]:
    """Parse ATCF lat like '152N' or '85S'."""
    s = s.strip()
    if not s:
        return None
    try:
        if s.endswith("N"):
            return float(s[:-1]) / 10.0
        if s.endswith("S"):
            return -float(s[:-1]) / 10.0
        return float(s) / 10.0
    except ValueError:
        return None


def _parse_atcf_lon(s: str) -> Optional[float]:
    """Parse ATCF lon like '1324E' or '1786W'."""
    s = s.strip()
    if not s:
        return None
    try:
        if s.endswith("E"):
            return float(s[:-1]) / 10.0
        if s.endswith("W"):
            return -float(s[:-1]) / 10.0
        return float(s) / 10.0
    except ValueError:
        return None


def _parse_atcf_time(dtg: str) -> Optional[datetime]:
    """Parse ATCF 10-digit DTG YYYYMMDDHH."""
    dtg = dtg.strip()
    if len(dtg) < 10:
        return None
    try:
        return datetime.strptime(dtg[:10], "%Y%m%d%H").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _parse_atcf_deck(raw_text: str) -> Dict[str, List[dict]]:
    """
    Parse an ATCF A-deck or B-deck text.
    Returns dict keyed by storm_id (basin+cy) → list of point dicts.
    """
    storms: Dict[str, List[dict]] = {}

    for line in raw_text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 8:
            continue

        basin = parts[0].upper()
        cy = parts[1].strip().zfill(2)
        dtg = parts[2].strip()
        tech = parts[4].strip() if len(parts) > 4 else ""
        tau_str = parts[5].strip() if len(parts) > 5 else "0"
        lat_str = parts[6].strip() if len(parts) > 6 else ""
        lon_str = parts[7].strip() if len(parts) > 7 else ""
        vmax_str = parts[8].strip() if len(parts) > 8 else ""
        mslp_str = parts[9].strip() if len(parts) > 9 else ""
        ty_str = parts[10].strip() if len(parts) > 10 else ""

        # Only ingest BEST or OFCL (official) entries; skip model entries
        if tech not in ("BEST", "OFCL", ""):
            continue

        lat = _parse_atcf_lat(lat_str)
        lon = _parse_atcf_lon(lon_str)
        if lat is None or lon is None:
            continue

        valid_time = _parse_atcf_time(dtg)
        tau = int(tau_str) if tau_str.isdigit() else 0

        try:
            vmax = float(vmax_str) if vmax_str else None
        except ValueError:
            vmax = None

        try:
            mslp = float(mslp_str) if mslp_str else None
        except ValueError:
            mslp = None

        storm_key = f"{basin}{cy}"
        if storm_key not in storms:
            storms[storm_key] = []

        storms[storm_key].append(
            {
                "basin": basin,
                "cy": cy,
                "valid_time": valid_time,
                "tau": tau,
                "lat": lat,
                "lon": lon,
                "vmax_kt": vmax,
                "mslp_hpa": mslp,
                "ty": ty_str,
                "tech": tech,
            }
        )

    return storms


def _fetch_jtwc_btk_all(year: int) -> Optional[str]:
    """
    Fetch all JTWC b-deck files (WP/IO/SH) for the current season from the
    NHC ATCF btk directory.

    Strategy:
    1. GET the directory listing HTML from https://ftp.nhc.noaa.gov/atcf/btk/
    2. Parse <a href="bwpNNyyyy.dat"> / <a href="bioNNyyyy.dat"> / <a href="bshNNyyyy.dat">
    3. Fetch each matched file and concatenate content.

    This avoids the incorrect `bwpall{year}.dat` filename that was used before
    (that format does not exist on NHC's server — the real files are per-storm:
    bwp01YYYY.dat, bwp02YYYY.dat, etc.).
    """
    import re

    try:
        dir_resp = HTTP_SESSION.get(NHC_ATCF_BTK_URL, timeout=15)
        dir_resp.raise_for_status()
    except Exception as exc:
        logger.warning(f"[JTWC] btk directory listing failed: {exc}")
        return None

    # Match filenames like bwp012026.dat, bio032026.dat, bsh022026.dat
    pattern = re.compile(
        rf'"(b(?:wp|io|sh)\d{{2}}{year}\.dat)"',
        re.IGNORECASE,
    )
    filenames = pattern.findall(dir_resp.text)

    if not filenames:
        logger.info(f"[JTWC] No WP/IO/SH b-deck files found in btk for {year} — basin is clear")
        return None

    logger.info(f"[JTWC] Found {len(filenames)} b-deck file(s): {filenames}")

    combined = ""
    for fname in filenames:
        try:
            r = HTTP_SESSION.get(NHC_ATCF_BTK_URL + fname, timeout=20)
            if r.status_code == 200 and r.text.strip():
                combined += r.text + "\n"
        except Exception as exc:
            logger.debug(f"[JTWC] Failed to fetch {fname}: {exc}")

    return combined.strip() or None


def run_jtwc_fetch_active():
    """Fetch active JTWC storms via ATCF b-deck from NHC btk mirror."""
    year = datetime.now(timezone.utc).year

    with sync_log(SOURCE, "jtwc_fetch_active_warnings") as log_data:
        all_text = _fetch_jtwc_btk_all(year)

        if not all_text:
            logger.info("[JTWC] No active JTWC-basin storms this season yet")
            return

        raw = all_text.encode()
        log_data["checksum"] = sha256_of_bytes(raw)
        log_data["raw_file_path"] = save_raw_file(SOURCE, "atcf_btk", "txt", raw)

        storm_data = _parse_atcf_deck(all_text)
        log_data["records_processed"] = len(storm_data)

        db = SessionLocal()
        try:
            _upsert_jtwc_storms(storm_data, db)
            db.commit()
        finally:
            db.close()

    cache_invalidate_storms()


def _upsert_jtwc_storms(storm_data: Dict[str, List[dict]], db):
    """Upsert storms parsed from ATCF into the DB."""
    current_year = datetime.now(timezone.utc).year
    active_keys = set()

    for storm_key, points in storm_data.items():
        if not points:
            continue

        # Filter to only current-year storms
        recent_points = [
            p for p in points
            if p["valid_time"] and p["valid_time"].year == current_year
        ]
        if not recent_points:
            continue

        # Get most recent best-track or forecast point
        best_points = [p for p in recent_points if p["tech"] == "BEST"]
        latest = best_points[-1] if best_points else recent_points[-1]

        basin_code = _map_basin(latest["basin"])
        source_id = storm_key.lower()

        storm = (
            db.query(Storm)
            .filter(Storm.source == SOURCE, Storm.source_storm_id == source_id)
            .first()
        )
        if storm is None:
            storm = Storm(source=SOURCE, source_storm_id=source_id)
            db.add(storm)
            db.flush()

        storm.basin = basin_code
        storm.is_active = True
        storm.status = "active"
        storm.lat = latest["lat"]
        storm.lon = latest["lon"]
        storm.wind_kt = latest["vmax_kt"]
        storm.pressure_hpa = latest["mslp_hpa"]
        storm.last_update_utc = datetime.now(timezone.utc)

        ty = latest["ty"]
        storm.category = ATCF_TY_CATEGORIES.get(ty) or (
            categorize_storm(latest["vmax_kt"], basin_code) if latest["vmax_kt"] else None
        )

        # Observed track from best-track points
        _rebuild_track(storm, best_points, db)

        # Compute movement from last two best-track points
        if len(best_points) >= 2:
            p1, p2 = best_points[-2], best_points[-1]
            if p1["valid_time"] and p2["valid_time"]:
                from utils.geo import compute_movement
                dir_deg, dir_text, speed_kt = compute_movement(
                    p1["lat"], p1["lon"], p1["valid_time"].timestamp(),
                    p2["lat"], p2["lon"], p2["valid_time"].timestamp(),
                )
                storm.movement_direction_deg = dir_deg
                storm.movement_direction_text = dir_text
                storm.movement_speed_kt = speed_kt

        active_keys.add(source_id)

    # Mark storms not in current fetch as inactive
    (
        db.query(Storm)
        .filter(
            Storm.source == SOURCE,
            Storm.is_active == True,
            ~Storm.source_storm_id.in_(active_keys),
        )
        .update({"is_active": False, "status": "dissipated"}, synchronize_session=False)
    )


def _rebuild_track(storm, points: list, db):
    """Replace all observed track points and rebuild LineString."""
    db.query(StormTrackPoint).filter(
        StormTrackPoint.storm_id == storm.id,
        StormTrackPoint.point_type == "observed",
    ).delete()
    db.query(StormTrack).filter(
        StormTrack.storm_id == storm.id,
        StormTrack.track_type == "observed",
    ).delete()

    coords = []
    prev = None  # {lat, lon, time_ts}
    for pt in points:
        coords.append([pt["lon"], pt["lat"]])
        category = ATCF_TY_CATEGORIES.get(pt["ty"]) or (
            categorize_storm(pt["vmax_kt"], storm.basin) if pt["vmax_kt"] else None
        )

        dir_deg, dir_text, speed_kt = None, None, None
        if prev is not None and pt["valid_time"] and prev["time_ts"]:
            dir_deg, dir_text, speed_kt = compute_movement(
                prev["lat"], prev["lon"], prev["time_ts"],
                pt["lat"], pt["lon"], pt["valid_time"].timestamp(),
            )

        db.add(
            StormTrackPoint(
                storm_id=storm.id,
                point_type="observed",
                valid_time_utc=pt["valid_time"],
                lat=pt["lat"],
                lon=pt["lon"],
                wind_kt=pt["vmax_kt"],
                pressure_hpa=pt["mslp_hpa"],
                category=category,
                movement_direction_deg=dir_deg,
                movement_direction_text=dir_text,
                movement_speed_kt=speed_kt,
            )
        )

        if pt["valid_time"]:
            prev = {"lat": pt["lat"], "lon": pt["lon"], "time_ts": pt["valid_time"].timestamp()}

    if len(coords) >= 2:
        db.add(
            StormTrack(
                storm_id=storm.id,
                track_type="observed",
                geojson={"type": "LineString", "coordinates": coords},
            )
        )

    # Update storm-level movement from last two observed points
    if len(points) >= 2:
        p1, p2 = points[-2], points[-1]
        if p1["valid_time"] and p2["valid_time"]:
            dir_deg, dir_text, speed_kt = compute_movement(
                p1["lat"], p1["lon"], p1["valid_time"].timestamp(),
                p2["lat"], p2["lon"], p2["valid_time"].timestamp(),
            )
            if dir_deg is not None:
                storm.movement_direction_deg = dir_deg
                storm.movement_direction_text = dir_text
                storm.movement_speed_kt = speed_kt


def _map_basin(atcf_basin: str) -> str:
    mapping = {
        "WP": "WP",
        "IO": "IO",
        "SH": "SP",
        "SP": "SP",
        "AL": "AL",
        "EP": "EP",
        "CP": "CP",
    }
    return mapping.get(atcf_basin.upper(), atcf_basin.upper())


def run_jtwc_crawler():
    logger.info("[JTWC] Starting crawler")
    try:
        run_jtwc_fetch_active()
    except Exception as e:
        logger.error(f"[JTWC] Crawler error: {e}")

    # Fetch satellite images for recently updated storms
    from crawlers.satellite_image_worker import fetch_and_store_for_storm
    db = SessionLocal()
    try:
        active_storms = db.query(Storm).filter(Storm.source == "JTWC", Storm.is_active == True).all()
        for storm in active_storms:
            try:
                n_imgs = fetch_and_store_for_storm(storm)
                if n_imgs:
                    logger.info(f"[JTWC] Stored {n_imgs} satellite image(s) for {storm.source_storm_id}")
            except Exception as e:
                logger.warning(f"[JTWC] Satellite image fetch failed for {storm.source_storm_id}: {e}")
    finally:
        db.close()

    logger.info("[JTWC] Done.")
