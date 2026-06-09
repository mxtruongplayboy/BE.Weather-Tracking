"""
NHC/NOAA Worker - P0
Covers: Atlantic (AL), East Pacific (EP), Central Pacific (CP)
Polling: active 10-15 min, forecast/cone 30 min, best track 1x/day
"""
import glob
import io
import json
import logging
import os
import zipfile
from datetime import datetime, timezone
from typing import Optional

import geopandas as gpd

from core.database import SessionLocal
from core.redis import cache_invalidate_storms
from crawlers.base_worker import (
    HTTP_SESSION,
    fetch_with_retry,
    save_raw_file,
    sha256_of_bytes,
    sync_log,
)
from models.storm_models import Storm, StormCone, StormTrack, StormTrackPoint
from utils.geo import compute_movement
from utils.units import categorize_storm

logger = logging.getLogger(__name__)

SOURCE = "NHC"

NHC_ACTIVE_URL = "https://www.nhc.noaa.gov/CurrentStorms.json"
NHC_GIS_ZIP_URL = "https://www.nhc.noaa.gov/gis/forecast/archive/{storm_id_upper}_5day_latest.zip"
NHC_ATCF_BTK_URL = "https://ftp.nhc.noaa.gov/atcf/btk/"

BASIN_MAP = {
    "al": "AL",
    "ep": "EP",
    "cp": "CP",
}


def _basin_from_id(storm_id: str) -> str:
    prefix = storm_id[:2].lower()
    return BASIN_MAP.get(prefix, "AL")


def _parse_nhc_intensity(raw_wind) -> Optional[float]:
    """CurrentStorms.json uses string knots like '65 kt' or just an int."""
    if raw_wind is None:
        return None
    if isinstance(raw_wind, (int, float)):
        return float(raw_wind)
    try:
        return float(str(raw_wind).split()[0])
    except (ValueError, IndexError):
        return None


def run_nhc_fetch_active():
    """Fetch and upsert all currently active NHC storms."""
    with sync_log(SOURCE, "nhc_fetch_active_storms") as log_data:
        resp = fetch_with_retry(NHC_ACTIVE_URL)
        raw = resp.content
        log_data["checksum"] = sha256_of_bytes(raw)
        log_data["raw_file_path"] = save_raw_file(SOURCE, "active_storms", "json", raw)

        active_list = resp.json().get("activeStorms", [])
        log_data["records_processed"] = len(active_list)

        db = SessionLocal()
        try:
            active_ids = set()

            for s in active_list:
                source_id = s.get("id", "").lower()
                if not source_id:
                    continue

                basin = _basin_from_id(source_id)
                name = s.get("name") or "Unknown"
                lat = s.get("latitudeNumeric")
                lon = s.get("longitudeNumeric")
                wind_kt = _parse_nhc_intensity(s.get("intensity"))
                pressure = s.get("pressure")

                category = categorize_storm(wind_kt, basin) if wind_kt else None

                storm = (
                    db.query(Storm)
                    .filter(Storm.source == SOURCE, Storm.source_storm_id == source_id)
                    .first()
                )
                if storm is None:
                    storm = Storm(source=SOURCE, source_storm_id=source_id)
                    db.add(storm)

                storm.name = name
                storm.basin = basin
                storm.is_active = True
                storm.status = "active"
                storm.lat = float(lat) if lat is not None else None
                storm.lon = float(lon) if lon is not None else None
                storm.wind_kt = wind_kt
                storm.pressure_hpa = float(pressure) if pressure is not None else None
                storm.category = category
                storm.last_update_utc = datetime.now(timezone.utc)
                storm.canonical_id = s.get("id", source_id).upper()

                active_ids.add(source_id)

            # Mark AL/EP/CP storms not in current response as inactive
            (
                db.query(Storm)
                .filter(
                    Storm.source == SOURCE,
                    Storm.is_active == True,
                    ~Storm.source_storm_id.in_(active_ids),
                )
                .update({"is_active": False, "status": "dissipated"}, synchronize_session=False)
            )

            db.commit()
        finally:
            db.close()

    cache_invalidate_storms()


def run_nhc_fetch_gis(source_storm_id: str):
    """Download 5-day GIS zip for a storm and upsert track/cone data."""
    storm_id_upper = source_storm_id.upper()
    zip_url = NHC_GIS_ZIP_URL.format(storm_id_upper=storm_id_upper)

    # Quick HEAD probe — GIS packages only exist for named storms with active
    # official advisories. Pre-named disturbances (e.g. EP022026) return 404
    # until NHC upgrades them to Tropical Depression/Storm status. Avoid the
    # 4-retry backoff loop (~30 s) and noisy ERROR logs for these expected 404s.
    try:
        probe = HTTP_SESSION.head(zip_url, timeout=8, allow_redirects=True)
        if probe.status_code == 404:
            logger.debug(f"[NHC] GIS not published yet for {storm_id_upper}")
            return
    except Exception:
        return  # Network blip — skip GIS silently this cycle

    with sync_log(SOURCE, f"nhc_fetch_gis_{source_storm_id}") as log_data:
        resp = fetch_with_retry(zip_url, timeout=30)

        raw = resp.content
        log_data["checksum"] = sha256_of_bytes(raw)
        log_data["raw_file_path"] = save_raw_file(SOURCE, f"gis_{storm_id_upper}", "zip", raw)

        db = SessionLocal()
        try:
            storm = (
                db.query(Storm)
                .filter(Storm.source == SOURCE, Storm.source_storm_id == source_storm_id)
                .first()
            )
            if storm is None:
                logger.warning(f"Storm {source_storm_id} not in DB, skipping GIS")
                return

            extract_dir = f"/tmp/nhc_{storm_id_upper}"
            os.makedirs(extract_dir, exist_ok=True)

            with zipfile.ZipFile(io.BytesIO(raw)) as z:
                z.extractall(extract_dir)

            _process_cone(storm, extract_dir, db)
            _process_forecast_pts(storm, extract_dir, db)
            _process_forecast_lin(storm, extract_dir, db)

            db.commit()
        finally:
            db.close()
            # Cleanup temp files
            for f in glob.glob(f"{extract_dir}/*"):
                try:
                    os.remove(f)
                except OSError:
                    pass
            try:
                os.rmdir(extract_dir)
            except OSError:
                pass

    cache_invalidate_storms()


def _process_cone(storm, extract_dir: str, db):
    pgn_files = glob.glob(f"{extract_dir}/*_5day_pgn.shp")
    if not pgn_files:
        return
    gdf = gpd.read_file(pgn_files[0])
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs("EPSG:4326")
    geojson = json.loads(gdf.to_json())

    db.query(StormCone).filter(
        StormCone.storm_id == storm.id,
        StormCone.cone_type == "forecast_cone",
    ).delete()

    cone = StormCone(
        storm_id=storm.id,
        cone_type="forecast_cone",
        geojson=geojson,
    )
    db.add(cone)


def _process_forecast_pts(storm, extract_dir: str, db):
    pts_files = glob.glob(f"{extract_dir}/*_5day_pts.shp")
    if not pts_files:
        return
    gdf = gpd.read_file(pts_files[0])
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs("EPSG:4326")

    db.query(StormTrackPoint).filter(
        StormTrackPoint.storm_id == storm.id,
        StormTrackPoint.point_type == "forecast",
    ).delete()

    prev_lat = prev_lon = prev_time = None
    for _, row in gdf.iterrows():
        lat = row.geometry.y
        lon = row.geometry.x
        tau = int(row.get("TAU", 0) or 0)

        valid_time = None
        if "VALIDTIME" in gdf.columns and row["VALIDTIME"]:
            try:
                valid_time = datetime.strptime(str(row["VALIDTIME"]), "%Y%m%d%H%M").replace(
                    tzinfo=timezone.utc
                )
            except ValueError:
                pass

        wind_kt = float(row["MAXWIND"]) if "MAXWIND" in gdf.columns and row["MAXWIND"] else None
        pressure = float(row["MINPRES"]) if "MINPRES" in gdf.columns and row["MINPRES"] else None
        category = categorize_storm(wind_kt, storm.basin) if wind_kt else None

        # Update storm movement from tau=0 → tau=12
        if tau == 0:
            prev_lat, prev_lon = lat, lon
            prev_time = valid_time.timestamp() if valid_time else None
        elif tau == 12 and prev_lat is not None and prev_time is not None and valid_time:
            dir_deg, dir_text, speed_kt = compute_movement(
                prev_lat, prev_lon, prev_time, lat, lon, valid_time.timestamp()
            )
            if dir_deg is not None:
                storm.movement_direction_deg = dir_deg
                storm.movement_direction_text = dir_text
                storm.movement_speed_kt = speed_kt

        pt = StormTrackPoint(
            storm_id=storm.id,
            point_type="forecast",
            forecast_hour=tau,
            valid_time_utc=valid_time,
            lat=lat,
            lon=lon,
            wind_kt=wind_kt,
            pressure_hpa=pressure,
            category=category,
        )
        db.add(pt)


def _process_forecast_lin(storm, extract_dir: str, db):
    lin_files = glob.glob(f"{extract_dir}/*_5day_lin.shp")
    if not lin_files:
        return
    gdf = gpd.read_file(lin_files[0])
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs("EPSG:4326")
    geojson = json.loads(gdf.to_json())

    db.query(StormTrack).filter(
        StormTrack.storm_id == storm.id,
        StormTrack.track_type == "forecast",
    ).delete()

    track = StormTrack(
        storm_id=storm.id,
        track_type="forecast",
        geojson=geojson,
    )
    db.add(track)


def _parse_atcf_lat(s: str) -> Optional[float]:
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
    dtg = dtg.strip()
    if len(dtg) < 10:
        return None
    try:
        return datetime.strptime(dtg[:10], "%Y%m%d%H").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _parse_nhc_btk(raw_text: str) -> list:
    """Parse ATCF b-deck text, returning BEST-track rows only."""
    points = []
    for line in raw_text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 8:
            continue
        tech = parts[4].strip() if len(parts) > 4 else ""
        if tech != "BEST":
            continue
        lat = _parse_atcf_lat(parts[6]) if len(parts) > 6 else None
        lon = _parse_atcf_lon(parts[7]) if len(parts) > 7 else None
        if lat is None or lon is None:
            continue
        valid_time = _parse_atcf_time(parts[2])
        try:
            vmax = float(parts[8]) if len(parts) > 8 and parts[8] else None
        except ValueError:
            vmax = None
        try:
            mslp = float(parts[9]) if len(parts) > 9 and parts[9] else None
        except ValueError:
            mslp = None
        ty = parts[10].strip() if len(parts) > 10 else ""
        points.append({
            "valid_time": valid_time,
            "lat": lat,
            "lon": lon,
            "vmax_kt": vmax,
            "mslp_hpa": mslp,
            "ty": ty,
        })
    return points


def run_nhc_fetch_track(source_storm_id: str):
    """Fetch ATCF b-deck file for a NHC storm and store its observed track.

    The b-deck filename mirrors the storm id directly:
      ep022026  →  bep022026.dat
    """
    fname = f"b{source_storm_id}.dat"
    url = NHC_ATCF_BTK_URL + fname

    # Quick HEAD probe — file may not exist yet for brand-new disturbances
    try:
        probe = HTTP_SESSION.head(url, timeout=8, allow_redirects=True)
        if probe.status_code == 404:
            logger.debug(f"[NHC] No btk file yet for {source_storm_id}")
            return
    except Exception:
        return

    with sync_log(SOURCE, f"nhc_fetch_track_{source_storm_id}") as log_data:
        try:
            resp = HTTP_SESSION.get(url, timeout=20)
            resp.raise_for_status()
        except Exception as exc:
            logger.warning(f"[NHC] btk fetch failed for {source_storm_id}: {exc}")
            return

        raw = resp.content
        if not raw.strip():
            return

        log_data["checksum"] = sha256_of_bytes(raw)
        log_data["raw_file_path"] = save_raw_file(
            SOURCE, f"btk_{source_storm_id}", "txt", raw
        )

        points = _parse_nhc_btk(resp.text)
        log_data["records_processed"] = len(points)
        if not points:
            return

        db = SessionLocal()
        try:
            storm = (
                db.query(Storm)
                .filter(
                    Storm.source == SOURCE,
                    Storm.source_storm_id == source_storm_id,
                )
                .first()
            )
            if storm is None:
                logger.warning(f"[NHC] Storm {source_storm_id} not in DB for track upsert")
                return

            # Replace existing observed track
            db.query(StormTrackPoint).filter(
                StormTrackPoint.storm_id == storm.id,
                StormTrackPoint.point_type == "observed",
            ).delete()
            db.query(StormTrack).filter(
                StormTrack.storm_id == storm.id,
                StormTrack.track_type == "observed",
            ).delete()

            coords = []
            for pt in points:
                category = (
                    categorize_storm(pt["vmax_kt"], storm.basin)
                    if pt["vmax_kt"]
                    else None
                )
                coords.append([pt["lon"], pt["lat"]])
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
                    )
                )

            if len(coords) >= 2:
                db.add(
                    StormTrack(
                        storm_id=storm.id,
                        track_type="observed",
                        geojson={"type": "LineString", "coordinates": coords},
                    )
                )

            # Update movement from last two best-track points if not already set
            if len(points) >= 2:
                p1, p2 = points[-2], points[-1]
                if (
                    p1["valid_time"]
                    and p2["valid_time"]
                    and storm.movement_direction_deg is None
                ):
                    dir_deg, dir_text, speed_kt = compute_movement(
                        p1["lat"],
                        p1["lon"],
                        p1["valid_time"].timestamp(),
                        p2["lat"],
                        p2["lon"],
                        p2["valid_time"].timestamp(),
                    )
                    if dir_deg is not None:
                        storm.movement_direction_deg = dir_deg
                        storm.movement_direction_text = dir_text
                        storm.movement_speed_kt = speed_kt

            db.commit()
            logger.info(
                f"[NHC] Stored {len(coords)} observed track point(s) for {source_storm_id}"
            )
        finally:
            db.close()

    cache_invalidate_storms()


def run_nhc_crawler():
    """Main entry point: fetch active + GIS for each storm."""
    logger.info("[NHC] Starting crawler")
    try:
        run_nhc_fetch_active()
    except Exception as e:
        logger.error(f"[NHC] Active fetch failed: {e}")
        return

    db = SessionLocal()
    try:
        active_storms = (
            db.query(Storm)
            .filter(Storm.source == SOURCE, Storm.is_active == True)
            .all()
        )
        storm_ids = [s.source_storm_id for s in active_storms]
    finally:
        db.close()

    for sid in storm_ids:
        try:
            run_nhc_fetch_gis(sid)
        except Exception as e:
            logger.warning(f"[NHC] GIS fetch failed for {sid}: {e}")
        try:
            run_nhc_fetch_track(sid)
        except Exception as e:
            logger.warning(f"[NHC] btk track fetch failed for {sid}: {e}")

    logger.info(f"[NHC] Done. Processed {len(storm_ids)} active storm(s).")
