"""
GFS Lightning Risk worker — Sprint 2.

Downloads selected GRIB2 variables from NOAA NOMADS GFS (1-degree global grid),
calculates a lightning risk score per grid point, and stores in lightning_risk_forecasts.

Variables used:
  - CAPE  (J/kg): convective instability
  - CPRAT (kg/m²/s × 3600 → mm/h): convective precipitation rate
  - PRATE (kg/m²/s × 3600 → mm/h): total precipitation rate
  - LI    (K): Best Lifted Index (optional)

Risk score formula (from spec §6.1):
  risk_score = 0.35*cape_score + 0.25*conv_precip_score + 0.15*precip_rate_score + 0.10*li_score
  Clamped to [0, 1].

Coverage: Global (0–360° lon, 90° to -90° lat).
"""
import logging
import math
import os
import tempfile
import warnings
from datetime import datetime, timedelta, timezone
from typing import Optional

# cfgrib/xarray emits a FutureWarning about compat='no_conflicts' default changing.
# This is a library-level warning, not a runtime error; suppress until cfgrib is updated.
warnings.filterwarnings(
    "ignore",
    message=".*default value for compat will change.*",
    category=FutureWarning,
    module="cfgrib",
)

from crawlers.base_worker import fetch_with_retry, save_raw_file, sha256_of_bytes, sync_log
from core.database import SessionLocal
from core.redis import cache_delete, cache_invalidate_lightning_tiles
from models.lightning_models import LightningRiskForecast

logger = logging.getLogger(__name__)

SOURCE = "GFS_RISK"
MODEL = "GFS"
RESOLUTION_DEG = 1.0

# NOMADS filter URL for GFS 1-degree pgrb2
NOMADS_BASE = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_1p00.pl"

# Forecast hours to fetch for MVP
FORECAST_HOURS = [0, 3, 6, 12, 24]

# Risk level thresholds (from spec §6.2)
RISK_LEVELS = [
    (0.75, "very_high"),
    (0.50, "high"),
    (0.25, "moderate"),
    (0.00, "low"),
]

RISK_MESSAGES = {
    "very_high": "Very high thunderstorm and lightning risk. Avoid open water and exposed areas.",
    "high": "High thunderstorm and lightning risk. Consider stopping outdoor/sea activity.",
    "moderate": "Moderate thunderstorm risk possible in the area.",
    "low": "Low thunderstorm risk.",
}


def _normalize(val: Optional[float], lo: float, hi: float) -> float:
    if val is None:
        return 0.0
    return max(0.0, min(1.0, (val - lo) / (hi - lo)))


def _normalize_negative(val: Optional[float], lo: float, hi: float) -> float:
    """For LI: lower (more negative) = more unstable = higher score."""
    if val is None:
        return 0.0
    return max(0.0, min(1.0, (lo - val) / (lo - hi)))


def _risk_score(cape: Optional[float], cprat_mmh: Optional[float],
                prate_mmh: Optional[float], li: Optional[float]) -> float:
    cape_s = _normalize(cape, 250, 2500)
    conv_s = _normalize(cprat_mmh, 0.5, 20)
    prec_s = _normalize(prate_mmh, 1, 50)
    li_s = _normalize_negative(li, -2, -8)

    # Weights sum to 0.85; remaining 0.15 reserved for history_prior (not implemented in MVP)
    score = 0.35 * cape_s + 0.25 * conv_s + 0.15 * prec_s + 0.10 * li_s
    return min(1.0, max(0.0, score))


def _risk_level(score: float) -> str:
    for threshold, level in RISK_LEVELS:
        if score >= threshold:
            return level
    return "low"


def _discover_latest_gfs_run() -> Optional[tuple[datetime, int]]:
    """
    Find the most recent available GFS run (00/06/12/18 UTC).
    Tries up to 4 runs back (24 h) to handle NOMADS delays or 500 errors.
    Returns (run_datetime, run_hour) or None.
    """
    now = datetime.now(timezone.utc)
    # GFS runs at 00, 06, 12, 18 UTC; data available ~4h after run time.
    # Build candidate list: align to 6-hour boundary, then step 6h backwards.
    aligned_hour = (now.hour // 6) * 6
    base = now.replace(hour=aligned_hour, minute=0, second=0, microsecond=0)
    candidates = [base - timedelta(hours=6 * i) for i in range(5)]

    for run_dt in candidates:
        run_hour = run_dt.hour
        date_str = run_dt.strftime("%Y%m%d")
        check_url = (
            f"{NOMADS_BASE}?file=gfs.t{run_hour:02d}z.pgrb2.1p00.f000"
            f"&var_CAPE=on&lev_surface=on"
            f"&subregion=&leftlon=0&rightlon=1&toplat=1&bottomlat=0"
            f"&dir=/gfs.{date_str}/{run_hour:02d}/atmos"
        )
        try:
            r = fetch_with_retry(check_url, timeout=15)
            if r.status_code == 200 and len(r.content) > 100:
                logger.info(f"[GFS RISK] Found available run: {run_dt.isoformat()}")
                return run_dt, run_hour
        except Exception:
            continue
    return None


def _download_gfs_grib2(run_dt: datetime, run_hour: int, fhour: int) -> Optional[bytes]:
    """
    Download a GRIB2 subset from NOMADS with CAPE, CPRAT, PRATE, LI at surface level.
    Returns raw bytes or None on failure.
    """
    date_str = run_dt.strftime("%Y%m%d")
    filename = f"gfs.t{run_hour:02d}z.pgrb2.1p00.f{fhour:03d}"

    # lev_surface covers CAPE, CPRAT, PRATE; LFTX is also at surface in GFS pgrb2.
    # lev_0-500_mb was removed — it is not a valid level for these variables and
    # causes NOMADS to return 500 when the filter cannot find matching records.
    url = (
        f"{NOMADS_BASE}?file={filename}"
        f"&var_CAPE=on&var_CPRAT=on&var_PRATE=on&var_LFTX=on"
        f"&lev_surface=on"
        f"&leftlon=0&rightlon=360&toplat=90&bottomlat=-90"
        f"&dir=/gfs.{date_str}/{run_hour:02d}/atmos"
    )
    try:
        resp = fetch_with_retry(url, timeout=120)
        if resp.status_code >= 500:
            logger.error(f"[GFS RISK] NOMADS 5xx for f{fhour:03d}: {resp.status_code}")
            return None
        return resp.content
    except Exception as e:
        logger.error(f"[GFS RISK] Download failed for f{fhour:03d}: {e}")
        return None


def _parse_grib2(data: bytes) -> list[dict]:
    """
    Parse GRIB2 bytes using cfgrib+xarray.
    Returns list of dicts: {lat, lon, cape, cprat, prate, li}.

    Root-cause fix: GFS 1° grid has separate 1-D lat (181) and lon (360) axes.
    The old code did zip(lat_1d, lon_1d) which stopped at 181 — missing 65 k
    grid points.  We now build a meshgrid so every (lat, lon) combination is
    visited, giving the correct 181 × 360 = 65 160 global points.

    Only points with risk_score > 0 are returned so that the caller does not
    write tens-of-thousands of useless "low / zero" records to the DB.
    """
    try:
        import cfgrib
        import numpy as np

        with tempfile.NamedTemporaryFile(suffix=".grb2", delete=False) as f:
            f.write(data)
            tmppath = f.name

        try:
            datasets = cfgrib.open_datasets(tmppath)

            merged: dict[str, np.ndarray] = {}
            lat_1d = lon_1d = None

            for ds in datasets:
                if lat_1d is None and "latitude" in ds.coords:
                    lat_1d = ds.coords["latitude"].values.copy()   # shape (181,)
                    lon_1d = ds.coords["longitude"].values.copy()  # shape (360,)
                for var in ds.data_vars:
                    merged[var.lower()] = ds[var].values.copy()    # shape (181, 360)
        finally:
            os.unlink(tmppath)

        if lat_1d is None:
            return []

        # Build full 2-D coordinate grids so every (lat, lon) cell is covered.
        # indexing='ij' → lats_2d[i,j] = lat_1d[i], lons_2d[i,j] = lon_1d[j]
        lats_2d, lons_2d = np.meshgrid(lat_1d, lon_1d, indexing="ij")
        lat_flat = lats_2d.flatten()   # 65 160 values
        lon_flat = lons_2d.flatten()   # 65 160 values

        cape_arr  = merged.get("cape",  merged.get("mcape"))
        cprat_arr = merged.get("cprat")
        prate_arr = merged.get("prate")
        lftx_arr  = merged.get("lftx",  merged.get("4lftx"))

        def _flat(arr):
            return arr.flatten() if arr is not None else None

        cape_f  = _flat(cape_arr)
        cprat_f = _flat(cprat_arr)
        prate_f = _flat(prate_arr)
        lftx_f  = _flat(lftx_arr)
        n = len(lat_flat)

        def _val(f, i):
            if f is None or i >= len(f):
                return None
            v = float(f[i])
            return None if math.isnan(v) else v

        result = []
        for i in range(n):
            c  = _val(cape_f,  i)
            cp = _val(cprat_f, i)
            pr = _val(prate_f, i)
            li = _val(lftx_f,  i)

            # Quick pre-filter: skip points with zero convective signal
            # (saves ~80-90 % of rows; low-risk areas are not stored at all).
            has_signal = (
                (c  is not None and c  > 50)   or   # CAPE > 50 J/kg
                (cp is not None and cp > 1e-6)  or   # any convective precip
                (li is not None and li < 0)          # unstable LI
            )
            if not has_signal:
                continue

            cp_mmh = cp * 3600 if cp is not None else None
            pr_mmh = pr * 3600 if pr is not None else None

            lon_norm = float(lon_flat[i]) - 360.0 if float(lon_flat[i]) > 180 else float(lon_flat[i])

            result.append({
                "lat": float(lat_flat[i]),
                "lon": lon_norm,
                "cape": c,
                "cprat_mmh": cp_mmh,
                "prate_mmh": pr_mmh,
                "li": li,
            })

        logger.info(f"[GFS RISK] Parsed {len(result):,} non-zero grid points from {n:,} total")
        return result

    except ImportError:
        logger.error("[GFS RISK] cfgrib/xarray not installed. Run: pip install cfgrib xarray eccodes")
        return []
    except Exception as e:
        logger.error(f"[GFS RISK] GRIB2 parse error: {e}")
        return []


def _store_risk_grid(points: list[dict], run_dt: datetime, valid_dt: datetime,
                     fhour: int, db) -> int:
    """Upsert risk grid into lightning_risk_forecasts."""
    if not points:
        return 0

    # Delete existing records for this run/valid_time to replace
    db.query(LightningRiskForecast).filter(
        LightningRiskForecast.model_source == MODEL,
        LightningRiskForecast.run_time_utc == run_dt,
        LightningRiskForecast.valid_time_utc == valid_dt,
    ).delete()

    BATCH = 5000
    total_inserted = 0
    batch = []

    for pt in points:
        score = _risk_score(pt["cape"], pt["cprat_mmh"], pt["prate_mmh"], pt["li"])
        if score <= 0:
            continue
        batch.append(LightningRiskForecast(
            model_source=MODEL,
            run_time_utc=run_dt,
            valid_time_utc=valid_dt,
            forecast_hour=fhour,
            resolution_deg=RESOLUTION_DEG,
            lat=pt["lat"],
            lon=pt["lon"],
            risk_score=score,
            risk_level=_risk_level(score),
            cape_jkg=pt["cape"],
            convective_precip_mm=pt["cprat_mmh"],
            precip_rate_mmh=pt["prate_mmh"],
            lifted_index=pt["li"],
        ))
        if len(batch) >= BATCH:
            db.bulk_save_objects(batch)
            db.commit()
            total_inserted += len(batch)
            batch = []

    if batch:
        db.bulk_save_objects(batch)
        db.commit()
        total_inserted += len(batch)

    return total_inserted


def run_gfs_risk_crawler():
    """
    Discover latest GFS run, download risk variables for each forecast hour,
    calculate risk grid, store to DB, invalidate cache.
    """
    logger.info("[GFS RISK] Starting crawler")

    result = _discover_latest_gfs_run()
    if result is None:
        logger.warning("[GFS RISK] No available GFS run found")
        return

    run_dt, run_hour = result
    logger.info(f"[GFS RISK] Using run: {run_dt.isoformat()}")

    total = 0
    for fhour in FORECAST_HOURS:
        valid_dt = run_dt + timedelta(hours=fhour)

        with sync_log(SOURCE, f"gfs_risk_f{fhour:03d}") as log_data:
            raw = _download_gfs_grib2(run_dt, run_hour, fhour)
            if raw is None:
                log_data["records_processed"] = 0
                continue

            log_data["checksum"] = sha256_of_bytes(raw)
            log_data["raw_file_path"] = save_raw_file(SOURCE, f"gfs_f{fhour:03d}", "grb2", raw)

            points = _parse_grib2(raw)
            if not points:
                log_data["records_processed"] = 0
                continue

            db = SessionLocal()
            try:
                n = _store_risk_grid(points, run_dt, valid_dt, fhour, db)
                log_data["records_processed"] = n
                total += n
                logger.info(f"[GFS RISK] f{fhour:03d}: {n} grid points stored")
            finally:
                db.close()

    # Invalidate risk cache and PNG tiles so next request gets fresh data
    cache_delete("lightning:risk:latest_run")
    cache_invalidate_lightning_tiles()
    logger.info(f"[GFS RISK] Done. Total grid points stored: {total}")


def query_risk_at_point(lat: float, lon: float, hours: int = 24,
                        run_dt: Optional[datetime] = None) -> list[dict]:
    """
    Return risk timeline for a given location by finding nearest grid point.
    Used by the API endpoint.
    """
    db = SessionLocal()
    try:
        # Find the latest run if not specified
        if run_dt is None:
            latest = (
                db.query(LightningRiskForecast.run_time_utc)
                .filter(LightningRiskForecast.model_source == MODEL)
                .order_by(LightningRiskForecast.run_time_utc.desc())
                .first()
            )
            if latest is None:
                return []
            run_dt = latest[0]

        max_valid = datetime.now(timezone.utc) + timedelta(hours=hours)

        # Find nearest grid point to requested lat/lon
        snap_lat = round(lat / RESOLUTION_DEG) * RESOLUTION_DEG
        snap_lon = round(lon / RESOLUTION_DEG) * RESOLUTION_DEG

        rows = (
            db.query(LightningRiskForecast)
            .filter(
                LightningRiskForecast.model_source == MODEL,
                LightningRiskForecast.run_time_utc == run_dt,
                LightningRiskForecast.lat.between(snap_lat - 0.6, snap_lat + 0.6),
                LightningRiskForecast.lon.between(snap_lon - 0.6, snap_lon + 0.6),
                LightningRiskForecast.valid_time_utc <= max_valid,
            )
            .order_by(LightningRiskForecast.forecast_hour)
            .all()
        )

        return [
            {
                "validTimeUtc": r.valid_time_utc,
                "forecastHour": r.forecast_hour,
                "riskScore": r.risk_score,
                "riskLevel": r.risk_level,
                "message": RISK_MESSAGES.get(r.risk_level, ""),
                "cape_jkg": r.cape_jkg,
                "convective_precip_mm": r.convective_precip_mm,
            }
            for r in rows
        ]
    finally:
        db.close()
