"""
Offline NASA LIS climatology importer.

Usage (inside container):
  docker cp /path/to/LISOTD_HRMC_V2.3.2014.hdf weather_tracking:/tmp/
  docker exec -it weather_tracking python3 scripts/import_nasa_lis.py

Or with a custom file path:
  docker exec -it weather_tracking python3 scripts/import_nasa_lis.py /tmp/myfile.hdf

Why offline: NASA GHRC (ghrc.nsstc.nasa.gov) blocks datacenter IPs.
Download the file on a local machine and upload via scp, then run this script.
"""
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_PATH = "/tmp/LISOTD_HRMC_V2.3.2014.hdf"


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PATH

    try:
        with open(path, "rb") as f:
            raw = f.read()
    except FileNotFoundError:
        logger.error(
            f"File not found: {path}\n"
            "Upload it first:\n"
            "  scp LISOTD_HRMC_V2.3.2014.hdf dev@<VPS_IP>:~/\n"
            "  docker cp ~/LISOTD_HRMC_V2.3.2014.hdf weather_tracking:/tmp/"
        )
        sys.exit(1)

    logger.info(f"Loaded {len(raw):,} bytes from {path}")

    from crawlers.nasa_lis_worker import _parse_hrmc_hdf5, _store_climatology
    from crawlers.base_worker import save_raw_file
    from core.database import SessionLocal

    save_raw_file("NASA_LIS", "nasa_lis_hrmc_manual", "hdf", raw)

    logger.info("Parsing HDF5 grid...")
    points = _parse_hrmc_hdf5(raw)
    logger.info(f"Parsed {len(points):,} grid points across 12 months")

    if not points:
        logger.error("No points parsed — file may be corrupt or wrong format")
        sys.exit(1)

    logger.info("Storing to database (may take a few minutes for large grids)...")
    db = SessionLocal()
    try:
        n = _store_climatology(points, db)
        logger.info(f"Done. Stored {n:,} climatology records.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
