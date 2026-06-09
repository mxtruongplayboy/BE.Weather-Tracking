"""
JMA Worker - P0
Covers: West Pacific (WP), South China Sea, Japan, Philippines
Polling: targetTc 15-30 min, TC detail/pastTracks 30-60 min

JMA Typhoon API (bosai):
  GET /bosai/typhoon/data/targetTc.json         → active TC id(s)
  GET /bosai/typhoon/data/targetTimes.json       → available forecast times
  GET /bosai/typhoon/data/{id}/pastTracks.json   → past track points
  GET /bosai/typhoon/data/{id}/TC{id}.json       → current detail + forecast + probability circles
"""
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from core.database import SessionLocal
from core.redis import cache_invalidate_storms
from crawlers.base_worker import (
    fetch_with_retry,
    save_raw_file,
    sha256_of_bytes,
    sync_log,
)
from models.storm_models import Storm, StormCone, StormTrack, StormTrackPoint
from utils.geo import compute_movement
from utils.units import categorize_storm, ms_to_kt

logger = logging.getLogger(__name__)

SOURCE = "JMA"
BASIN = "WP"

JMA_BASE = "https://www.jma.go.jp/bosai/typhoon/data"
JMA_HEADERS = {
    "Referer": "https://www.jma.go.jp/bosai/map.html",
    "X-Requested-With": "XMLHttpRequest",
}

JMA_GRADE_MAP = {
    "VT": "violent_typhoon",       # ≥105 kt
    "TY": "typhoon",               # ≥64 kt
    "STS": "severe_tropical_storm",# ≥48 kt
    "TS": "tropical_storm",        # ≥34 kt
    "TD": "tropical_depression",
    "TW": "tropical_depression",
}


def _parse_jma_time(s: str) -> Optional[datetime]:
    """Parse JMA ISO-8601 time string to UTC datetime."""
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def _kt_from_jma(wind_ms: Any) -> Optional[float]:
    """JMA reports wind in m/s; convert to kt."""
    if wind_ms is None:
        return None
    try:
        return round(ms_to_kt(float(wind_ms)), 1)
    except (ValueError, TypeError):
        return None


def _category_from_grade(grade: str) -> str:
    return JMA_GRADE_MAP.get(grade, "tropical_storm")


def run_jma_fetch_target_tc() -> List[str]:
    """Fetch active TC IDs from JMA. Returns list of TC IDs."""
    with sync_log(SOURCE, "jma_fetch_target_tc") as log_data:
        try:
            resp = fetch_with_retry(
                f"{JMA_BASE}/targetTc.json",
                extra_headers=JMA_HEADERS,
            )
        except Exception as e:
            logger.warning(f"[JMA] targetTc fetch error: {e}")
            raise

        raw = resp.content
        log_data["checksum"] = sha256_of_bytes(raw)
        log_data["raw_file_path"] = save_raw_file(SOURCE, "targetTc", "json", raw)

        data = resp.json()

        # JMA returns either a list or a dict with "targetTc" key
        if isinstance(data, list):
            tc_ids = [item.get("id") or item for item in data if item]
        elif isinstance(data, dict):
            tc_ids = data.get("targetTc", [])
            if isinstance(tc_ids, str):
                tc_ids = [tc_ids] if tc_ids else []
        else:
            tc_ids = []

        tc_ids = [str(t) for t in tc_ids if t]
        log_data["records_processed"] = len(tc_ids)

        if not tc_ids:
            logger.info("[JMA] No active typhoon.")
            # Mark all JMA storms inactive
            db = SessionLocal()
            try:
                db.query(Storm).filter(
                    Storm.source == SOURCE, Storm.is_active == True
                ).update({"is_active": False, "status": "dissipated"}, synchronize_session=False)
                db.commit()
            finally:
                db.close()

        return tc_ids


def run_jma_fetch_tc_detail(tc_id: str):
    """Fetch past tracks, current detail, and forecast for one JMA TC."""
    _fetch_past_tracks(tc_id)
    _fetch_tc_json(tc_id)
    cache_invalidate_storms()


def _fetch_past_tracks(tc_id: str):
    url = f"{JMA_BASE}/{tc_id}/pastTracks.json"
    with sync_log(SOURCE, f"jma_fetch_past_tracks_{tc_id}") as log_data:
        resp = fetch_with_retry(url, extra_headers=JMA_HEADERS)
        raw = resp.content
        log_data["checksum"] = sha256_of_bytes(raw)
        log_data["raw_file_path"] = save_raw_file(SOURCE, f"pastTracks_{tc_id}", "json", raw)

        data = resp.json()
        if not data:
            return

        db = SessionLocal()
        try:
            storm = _get_or_create_storm(db, tc_id)

            # Delete old observed points
            db.query(StormTrackPoint).filter(
                StormTrackPoint.storm_id == storm.id,
                StormTrackPoint.point_type == "observed",
            ).delete()

            points_raw = data if isinstance(data, list) else data.get("track", [])
            coords = []
            prev = None

            for pt in points_raw:
                lat = float(pt.get("lat", 0))
                lon = float(pt.get("lon", 0))
                valid_time = _parse_jma_time(pt.get("time") or pt.get("datetime"))
                wind_kt = _kt_from_jma(pt.get("wind") or pt.get("windSpeed"))
                pressure = float(pt.get("pressure", 0) or 0) or None
                grade = pt.get("grade") or pt.get("class") or ""
                category = _category_from_grade(grade) if grade else (
                    categorize_storm(wind_kt, BASIN) if wind_kt else None
                )

                coords.append([lon, lat])

                db.add(
                    StormTrackPoint(
                        storm_id=storm.id,
                        point_type="observed",
                        valid_time_utc=valid_time,
                        lat=lat,
                        lon=lon,
                        wind_kt=wind_kt,
                        pressure_hpa=pressure,
                        category=category,
                    )
                )

                # Compute movement from last two points
                if prev is not None and valid_time:
                    prev_time_ts = prev["time"].timestamp() if prev["time"] else None
                    if prev_time_ts:
                        dir_deg, dir_text, speed_kt = compute_movement(
                            prev["lat"], prev["lon"], prev_time_ts,
                            lat, lon, valid_time.timestamp(),
                        )
                        storm.movement_direction_deg = dir_deg
                        storm.movement_direction_text = dir_text
                        storm.movement_speed_kt = speed_kt

                prev = {"lat": lat, "lon": lon, "time": valid_time}

            # Rebuild observed LineString
            db.query(StormTrack).filter(
                StormTrack.storm_id == storm.id,
                StormTrack.track_type == "observed",
            ).delete()
            if len(coords) >= 2:
                geojson = {"type": "LineString", "coordinates": coords}
                db.add(
                    StormTrack(
                        storm_id=storm.id,
                        track_type="observed",
                        geojson=geojson,
                    )
                )

            log_data["records_processed"] = len(points_raw)
            db.commit()
        finally:
            db.close()


def _fetch_tc_json(tc_id: str):
    url = f"{JMA_BASE}/{tc_id}/TC{tc_id}.json"
    with sync_log(SOURCE, f"jma_fetch_tc_detail_{tc_id}") as log_data:
        resp = fetch_with_retry(url, extra_headers=JMA_HEADERS)
        raw = resp.content
        log_data["checksum"] = sha256_of_bytes(raw)
        log_data["raw_file_path"] = save_raw_file(SOURCE, f"TC{tc_id}", "json", raw)

        data = resp.json()
        if not data:
            return

        db = SessionLocal()
        try:
            storm = _get_or_create_storm(db, tc_id)

            # Current position from the JSON
            current = data.get("current") or {}
            name = data.get("name") or data.get("eName") or tc_id
            storm.name = name
            storm.basin = BASIN
            storm.is_active = True
            storm.status = "active"
            storm.last_update_utc = datetime.now(timezone.utc)

            if current:
                storm.lat = float(current.get("lat") or 0) or None
                storm.lon = float(current.get("lon") or 0) or None
                storm.wind_kt = _kt_from_jma(current.get("wind") or current.get("windSpeed"))
                pressure = current.get("pressure")
                storm.pressure_hpa = float(pressure) if pressure else None
                grade = current.get("grade") or current.get("class") or ""
                storm.category = _category_from_grade(grade) if grade else (
                    categorize_storm(storm.wind_kt, BASIN) if storm.wind_kt else None
                )

            # Forecast track (track1 = best estimate, track2 = alternate)
            forecast_tracks = data.get("track1") or data.get("forecast") or []
            _upsert_forecast_track(storm, forecast_tracks, db)

            # Probability circles
            prob_circles = data.get("probabilityCircle") or data.get("probCircle") or []
            _upsert_probability_circles(storm, prob_circles, db)

            log_data["records_processed"] = 1
            db.commit()
        finally:
            db.close()


def _upsert_forecast_track(storm, forecast_list: list, db):
    if not forecast_list:
        return

    db.query(StormTrackPoint).filter(
        StormTrackPoint.storm_id == storm.id,
        StormTrackPoint.point_type == "forecast",
    ).delete()
    db.query(StormTrack).filter(
        StormTrack.storm_id == storm.id,
        StormTrack.track_type == "forecast",
    ).delete()

    coords = []
    for item in forecast_list:
        lat = float(item.get("lat") or 0)
        lon = float(item.get("lon") or 0)
        tau = int(item.get("tau") or item.get("fcstHour") or 0)
        valid_time = _parse_jma_time(item.get("time") or item.get("validTime"))
        wind_kt = _kt_from_jma(item.get("wind") or item.get("windSpeed"))
        pressure = item.get("pressure")
        grade = item.get("grade") or item.get("class") or ""
        category = _category_from_grade(grade) if grade else (
            categorize_storm(wind_kt, BASIN) if wind_kt else None
        )

        coords.append([lon, lat])
        db.add(
            StormTrackPoint(
                storm_id=storm.id,
                point_type="forecast",
                forecast_hour=tau,
                valid_time_utc=valid_time,
                lat=lat,
                lon=lon,
                wind_kt=wind_kt,
                pressure_hpa=float(pressure) if pressure else None,
                category=category,
            )
        )

    if len(coords) >= 2:
        db.add(
            StormTrack(
                storm_id=storm.id,
                track_type="forecast",
                geojson={"type": "LineString", "coordinates": coords},
            )
        )


def _upsert_probability_circles(storm, circles: list, db):
    if not circles:
        return

    db.query(StormCone).filter(
        StormCone.storm_id == storm.id,
        StormCone.cone_type == "probability_circle",
    ).delete()

    for circle in circles:
        tau = int(circle.get("tau") or circle.get("fcstHour") or 0)
        lat = float(circle.get("lat") or 0)
        lon = float(circle.get("lon") or 0)
        radius_km = float(circle.get("radius") or circle.get("radiusKm") or 0)

        # Represent as a GeoJSON Point + radius property (mobile renders as circle)
        geojson = {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {
                "radiusKm": radius_km,
                "forecastHour": tau,
                "coneType": "probability_circle",
            },
        }
        db.add(
            StormCone(
                storm_id=storm.id,
                cone_type="probability_circle",
                forecast_hour=tau,
                geojson=geojson,
            )
        )


def _get_or_create_storm(db, tc_id: str) -> Storm:
    storm = (
        db.query(Storm)
        .filter(Storm.source == SOURCE, Storm.source_storm_id == tc_id)
        .first()
    )
    if storm is None:
        storm = Storm(
            source=SOURCE,
            source_storm_id=tc_id,
            basin=BASIN,
        )
        db.add(storm)
        db.flush()
    return storm


def run_jma_crawler():
    """Main entry: fetch active TCs then fetch detail for each."""
    logger.info("[JMA] Starting crawler")
    try:
        tc_ids = run_jma_fetch_target_tc()
    except Exception as e:
        logger.error(f"[JMA] targetTc failed: {e}")
        return

    for tc_id in tc_ids:
        try:
            run_jma_fetch_tc_detail(tc_id)
            logger.info(f"[JMA] Processed TC {tc_id}")
        except Exception as e:
            logger.error(f"[JMA] Detail fetch failed for {tc_id}: {e}")

    logger.info(f"[JMA] Done. {len(tc_ids)} active TC(s).")
