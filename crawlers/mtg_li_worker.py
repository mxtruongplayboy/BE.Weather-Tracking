"""
EUMETSAT MTG Lightning Imager (MTG LI) worker — Sprint 4.

Coverage: Europe, Africa, Middle East, and adjacent areas.
Access: Requires EUMETSAT account + Data Store API credentials.
        Fails gracefully when credentials are not configured.

Auth flow:
  1. POST to EUMETSAT token endpoint with consumer_key + consumer_secret
  2. Use Bearer token for Data Store API queries
  3. Refresh token before expiry

Set env vars to enable:
  EUMETSAT_CONSUMER_KEY=<your_key>
  EUMETSAT_CONSUMER_SECRET=<your_secret>
"""
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from crawlers.base_worker import save_raw_file, sha256_of_bytes, sync_log
from core.database import SessionLocal
from core.redis import cache_delete
from models.lightning_models import LightningEvent

logger = logging.getLogger(__name__)

SOURCE = "MTG_LI"
SATELLITE = "MTG_I1"
PRODUCT = "LI-L2-LGR"

# EUMETSAT Data Store API
TOKEN_URL = "https://api.eumetsat.int/token"
DATA_STORE_URL = "https://api.eumetsat.int/data/browse/1.0.0/collections"
LI_COLLECTION = "EO:EUM:DAT:0669"   # MTG LI Level 2 Lightning Group Radiances

MTG_COVERAGE_NOTE = (
    "Real lightning strike data available for Europe, Africa, Middle East "
    "via EUMETSAT MTG LI when credentials are configured."
)


def _credentials_available() -> bool:
    return bool(
        os.getenv("EUMETSAT_CONSUMER_KEY")
        and os.getenv("EUMETSAT_CONSUMER_SECRET")
    )


def _get_access_token() -> Optional[str]:
    """Obtain Bearer token from EUMETSAT OAuth2 endpoint."""
    import requests
    key = os.getenv("EUMETSAT_CONSUMER_KEY")
    secret = os.getenv("EUMETSAT_CONSUMER_SECRET")
    if not key or not secret:
        return None
    try:
        resp = requests.post(
            TOKEN_URL,
            data={"grant_type": "client_credentials"},
            auth=(key, secret),
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json().get("access_token")
    except Exception as e:
        logger.error(f"[MTG LI] Token fetch failed: {e}")
        return None


def _discover_latest_products(token: str, since_minutes: int = 60) -> list[dict]:
    """
    Query EUMETSAT Data Store for recent MTG LI Level-2 products.
    Returns list of product dicts with download URL.
    """
    import requests
    since = (datetime.now(timezone.utc) - timedelta(minutes=since_minutes)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    url = (
        f"{DATA_STORE_URL}/{LI_COLLECTION}/products"
        f"?dtstart={since}&si=0&c=5&sort=start,+1"
    )
    try:
        resp = requests.get(
            url, headers={"Authorization": f"Bearer {token}"}, timeout=20
        )
        resp.raise_for_status()
        return resp.json().get("products", [])
    except Exception as e:
        logger.error(f"[MTG LI] Product discovery failed: {e}")
        return []


def _download_product(token: str, product: dict) -> Optional[bytes]:
    """Download a single MTG LI product file."""
    import requests
    download_url = product.get("url") or product.get("downloadUrl")
    if not download_url:
        return None
    try:
        resp = requests.get(
            download_url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=120,
            stream=True,
        )
        resp.raise_for_status()
        return resp.content
    except Exception as e:
        logger.error(f"[MTG LI] Product download failed: {e}")
        return None


def _parse_mtg_netcdf(data: bytes, product_id: str) -> list[dict]:
    """Parse MTG LI Level-2 NetCDF product, extract flash/group events."""
    try:
        import netCDF4 as nc
        import numpy as np

        ds = nc.Dataset("inmemory.nc", memory=data)
        events = []
        try:
            # MTG LI L2 variables (field names may vary by product version)
            lat_var = ds.variables.get("flash_lat") or ds.variables.get("lat")
            lon_var = ds.variables.get("flash_lon") or ds.variables.get("lon")
            time_var = ds.variables.get("flash_time") or ds.variables.get("time")

            if lat_var is None or lon_var is None:
                logger.warning(f"[MTG LI] No lat/lon variables in product {product_id}")
                return []

            lats = lat_var[:]
            lons = lon_var[:]
            times = time_var[:] if time_var is not None else None

            energy_var = ds.variables.get("flash_energy") or ds.variables.get("energy")

            ref_time = datetime(1970, 1, 1, tzinfo=timezone.utc)
            if time_var is not None and hasattr(time_var, "units"):
                try:
                    import netCDF4 as nc4
                    ref_time = nc4.num2date(0, time_var.units).replace(tzinfo=timezone.utc)
                except Exception:
                    pass

            for i in range(len(lats)):
                lat = float(lats[i])
                lon = float(lons[i])
                if np.ma.is_masked(lat) or np.ma.is_masked(lon):
                    continue

                t = ref_time
                if times is not None:
                    try:
                        t = ref_time + timedelta(seconds=float(times[i]))
                    except Exception:
                        pass

                energy = None
                if energy_var is not None:
                    v = float(energy_var[i])
                    if not np.isnan(v):
                        energy = v

                events.append({
                    "source_event_id": f"{SATELLITE}_{product_id}_{i}",
                    "lat": lat,
                    "lon": lon,
                    "time_utc": t,
                    "energy": energy,
                })
        finally:
            ds.close()

        return events

    except ImportError:
        logger.error("[MTG LI] netCDF4 not installed — cannot parse LI files")
        return []
    except Exception as e:
        logger.error(f"[MTG LI] Parse error for {product_id}: {e}")
        return []


def _upsert_events(events: list[dict], db) -> int:
    inserted = 0
    for ev in events:
        existing = db.query(LightningEvent).filter(
            LightningEvent.source == SOURCE,
            LightningEvent.source_event_id == ev["source_event_id"],
        ).first()
        if existing:
            continue
        db.add(LightningEvent(
            source=SOURCE,
            source_event_id=ev["source_event_id"],
            satellite=SATELLITE,
            product=PRODUCT,
            event_type="flash",
            time_utc=ev["time_utc"],
            lat=ev["lat"],
            lon=ev["lon"],
            energy=ev.get("energy"),
        ))
        inserted += 1

    if inserted:
        db.commit()
    return inserted


def run_mtg_li_crawler():
    """
    Authenticate with EUMETSAT, discover and ingest recent MTG LI products.
    Fails gracefully when credentials are not configured.
    """
    if not _credentials_available():
        logger.debug(
            "[MTG LI] Credentials not configured (EUMETSAT_CONSUMER_KEY / EUMETSAT_CONSUMER_SECRET). "
            "Skipping MTG LI ingestion."
        )
        return

    logger.info("[MTG LI] Starting crawler")

    token = _get_access_token()
    if token is None:
        logger.error("[MTG LI] Could not obtain access token")
        return

    products = _discover_latest_products(token, since_minutes=30)
    if not products:
        logger.info("[MTG LI] No new products found")
        return

    total = 0
    for product in products:
        product_id = product.get("id", "unknown")
        with sync_log(SOURCE, "mtg_li_ingest") as log_data:
            raw = _download_product(token, product)
            if raw is None:
                continue

            log_data["checksum"] = sha256_of_bytes(raw)
            log_data["raw_file_path"] = save_raw_file(SOURCE, "mtg_li", "nc", raw)

            events = _parse_mtg_netcdf(raw, product_id)
            db = SessionLocal()
            try:
                n = _upsert_events(events, db)
                log_data["records_processed"] = n
                total += n
            finally:
                db.close()

    cache_delete("lightning:events:recent:MTG_LI:15")
    cache_delete("lightning:events:recent:MTG_LI:30")
    logger.info(f"[MTG LI] Done. Total new events: {total}")
