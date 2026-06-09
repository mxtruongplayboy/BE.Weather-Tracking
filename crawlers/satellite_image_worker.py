"""
Storm Satellite Image Worker.

Fetches satellite imagery from NASA GIBS WMTS for each observed storm track point
and stores the raw tile bytes in the storm_satellite_images table.

Source: NASA GIBS (Global Imagery Browse Services)
  - No auth required, public API
  - Historical date support (crucial for past track points)
  - URL: https://gibs.earthdata.nasa.gov/wmts/epsg4326/best/{layer}/default/{date}/2km/{zoom}/{row}/{col}.jpg

Layer selection by basin:
  AL (Atlantic)          → GOES-East  (GOES-16, 75.2°W)
  EP, CP (East Pacific)  → GOES-West  (GOES-18, 137.2°W)
  WP (West Pacific)      → Himawari   (140.7°E) — falls back to VIIRS if tile empty
  IO, SH, others         → VIIRS SNPP (global polar orbit, fallback)

Limits per crawler run: max MAX_FETCH_PER_STORM new images per storm to avoid blocking.
Only observed (past) track points are fetched — forecast points are future, no imagery exists.
"""
import logging
import math
from datetime import datetime, timedelta, timezone
from typing import Optional

from core.database import SessionLocal
from crawlers.base_worker import fetch_with_retry
from models.storm_models import Storm, StormSatelliteImage, StormTrackPoint

logger = logging.getLogger(__name__)

GIBS_BASE = "https://gibs.earthdata.nasa.gov/wmts/epsg4326/best"
TILE_MATRIX_SET = "2km"
ZOOM = 4          # ~11.25° per tile — shows ~1000km region centred on storm
MAX_FETCH_PER_STORM = 5    # max new images per crawler run per storm
LOOKBACK_HOURS = 72        # only fetch images for points within this window

# Basin → (layer_name, satellite_source, format)
LAYER_MAP = {
    "AL": ("GOES-East_ABI_GeoColor",     "GOES_EAST", "jpg"),
    "EP": ("GOES-West_ABI_GeoColor",     "GOES_WEST", "jpg"),
    "CP": ("GOES-West_ABI_GeoColor",     "GOES_WEST", "jpg"),
    "WP": ("Himawari_AHI_Band03_Red",    "HIMAWARI",  "jpg"),
    "IO": ("VIIRS_SNPP_CorrectedReflectance_TrueColor", "VIIRS", "jpg"),
    "SH": ("VIIRS_SNPP_CorrectedReflectance_TrueColor", "VIIRS", "jpg"),
}
FALLBACK_LAYER = ("VIIRS_SNPP_CorrectedReflectance_TrueColor", "VIIRS", "jpg")


def _lat_lon_to_tile(lat: float, lon: float, zoom: int) -> tuple[int, int]:
    """Convert WGS84 lat/lon to GIBS WMTS EPSG:4326 tile (col, row) at given zoom."""
    # At zoom N: 2^(N+1) columns, 2^N rows; each tile = 180/2^N degrees
    n_cols = 2 ** (zoom + 1)   # 32 at zoom=4
    n_rows = 2 ** zoom          # 16 at zoom=4
    col = int((lon + 180.0) / 360.0 * n_cols) % n_cols
    row = int((90.0 - lat) / 180.0 * n_rows)
    row = max(0, min(n_rows - 1, row))
    return col, row


def _gibs_url(layer: str, date_str: str, zoom: int, col: int, row: int, fmt: str) -> str:
    return f"{GIBS_BASE}/{layer}/default/{date_str}/{TILE_MATRIX_SET}/{zoom}/{row}/{col}.{fmt}"


def _fetch_tile(lat: float, lon: float, time_utc: datetime, basin: str
                ) -> tuple[bytes, str, str, int, int] | None:
    """
    Download one GIBS WMTS tile for the given position and time.
    Returns (image_bytes, satellite_source, layer_name, col, row) or None.
    """
    layer, source, fmt = LAYER_MAP.get((basin or "").upper(), FALLBACK_LAYER)
    date_str = time_utc.strftime("%Y-%m-%d")
    col, row = _lat_lon_to_tile(lat, lon, ZOOM)
    url = _gibs_url(layer, date_str, ZOOM, col, row, fmt)

    try:
        resp = fetch_with_retry(url, timeout=30)
        data = resp.content
        # GIBS returns a tiny "no data" tile (~1-2KB) when no imagery exists
        if len(data) < 3000:
            # If primary layer returned empty tile, try VIIRS fallback
            if source != "VIIRS":
                fb_layer, fb_source, fb_fmt = FALLBACK_LAYER
                fb_url = _gibs_url(fb_layer, date_str, ZOOM, col, row, fb_fmt)
                try:
                    fb_resp = fetch_with_retry(fb_url, timeout=30)
                    fb_data = fb_resp.content
                    if len(fb_data) >= 3000:
                        return fb_data, fb_source, fb_layer, col, row
                except Exception:
                    pass
            return None   # genuinely no imagery
        return data, source, layer, col, row
    except Exception as e:
        logger.warning(f"[SAT IMG] Tile fetch failed ({basin} {lat:.1f},{lon:.1f} {date_str}): {e}")
        return None


def fetch_and_store_for_storm(storm: Storm) -> int:
    """
    Fetch satellite images for observed track points of a storm that have no image yet.
    Called at the end of each crawler run per storm.
    Returns number of images newly stored.
    """
    db = SessionLocal()
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)

        # IDs that already have an image record (success or failure)
        already_fetched = {
            row[0]
            for row in db.query(StormSatelliteImage.track_point_id)
            .filter(StormSatelliteImage.storm_id == storm.id)
            .all()
            if row[0] is not None
        }

        # Observed track points within lookback window, newest first, not yet processed
        points = (
            db.query(StormTrackPoint)
            .filter(
                StormTrackPoint.storm_id == storm.id,
                StormTrackPoint.point_type == "observed",
                StormTrackPoint.valid_time_utc >= cutoff,
                ~StormTrackPoint.id.in_(already_fetched),
            )
            .order_by(StormTrackPoint.valid_time_utc.desc())
            .limit(MAX_FETCH_PER_STORM)
            .all()
        )

        if not points:
            return 0

        stored = 0
        for pt in points:
            if pt.lat is None or pt.lon is None or pt.valid_time_utc is None:
                continue

            result = _fetch_tile(pt.lat, pt.lon, pt.valid_time_utc, storm.basin or "")

            img = StormSatelliteImage(
                storm_id=storm.id,
                track_point_id=pt.id,
                point_type="observed",
                time_utc=pt.valid_time_utc,
                lat=pt.lat,
                lon=pt.lon,
                basin=storm.basin,
                zoom_level=ZOOM,
            )

            if result:
                img_bytes, sat_src, layer, col, row = result
                img.image_data = img_bytes
                img.satellite_source = sat_src
                img.layer_name = layer
                img.tile_col = col
                img.tile_row = row
                img.image_format = "jpeg"
                img.image_size_bytes = len(img_bytes)
                logger.info(
                    f"[SAT IMG] Stored {sat_src} image for storm {storm.name or storm.id} "
                    f"at {pt.valid_time_utc.strftime('%Y-%m-%d %H:%M')} UTC "
                    f"({len(img_bytes)//1024}KB)"
                )
            else:
                img.satellite_source = "UNKNOWN"
                img.layer_name = "none"
                img.fetch_error = f"No imagery available for basin {storm.basin} on {pt.valid_time_utc.strftime('%Y-%m-%d')}"
                logger.debug(f"[SAT IMG] No imagery for track point {pt.id} ({pt.valid_time_utc})")

            db.add(img)
            stored += 1

        if stored:
            db.commit()

        return stored

    except Exception as e:
        logger.error(f"[SAT IMG] Error fetching images for storm {storm.id}: {e}")
        return 0
    finally:
        db.close()
