import asyncio
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

from core.database import Base, engine, init_postgis
from models import storm_models       # noqa: F401 — registers Storm ORM models
from models import lightning_models   # noqa: F401 — registers Lightning ORM models

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Weather Tracking API",
    version="3.0.0",
    description=(
        "Global tropical cyclone + lightning risk tracking backend. "
        "Sources: NOAA/NHC, JMA, JTWC, IBTrACS, NOAA GOES GLM, "
        "EUMETSAT MTG LI, NASA LIS/OTD, NOAA GFS."
    ),
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ───────────────────────────────────────────────────────────────────

from api import admin, health, storms, hazards
from api.lightning import router as lightning_router, tiles_router as lightning_tiles_router

app.include_router(storms.router)
app.include_router(storms.map_router)
app.include_router(health.router)
app.include_router(admin.router)
app.include_router(lightning_router)
app.include_router(lightning_tiles_router)
app.include_router(hazards.router)

# ── Scheduler ─────────────────────────────────────────────────────────────────

from crawlers import (
    ibtracs_worker, jma_worker, jtwc_worker, nhc_worker,
    goes_glm_worker, gfs_risk_worker, mtg_li_worker, nasa_lis_worker,
)

scheduler = AsyncIOScheduler(timezone="UTC")


def _seed_lightning_sources():
    """Insert lightning sources into DB if not already present."""
    from core.database import SessionLocal
    from models.lightning_models import LightningSource, LIGHTNING_SOURCES

    db = SessionLocal()
    try:
        for src in LIGHTNING_SOURCES:
            existing = db.query(LightningSource).filter(
                LightningSource.code == src["code"]
            ).first()
            if not existing:
                db.add(LightningSource(**src))
        db.commit()
    except Exception as e:
        logger.warning(f"Source seeding error: {e}")
    finally:
        db.close()


@app.on_event("startup")
async def startup_event():
    logger.info("Initializing database…")
    try:
        init_postgis(engine)
    except Exception as e:
        logger.warning(f"PostGIS init warning (may already be enabled): {e}")

    Base.metadata.create_all(bind=engine)

    # Migrations: add per-point movement columns to storm_track_points
    with engine.connect() as conn:
        for col, typ in [
            ("movement_direction_deg", "FLOAT"),
            ("movement_direction_text", "VARCHAR(5)"),
            ("movement_speed_kt", "FLOAT"),
        ]:
            conn.execute(text(
                f"ALTER TABLE storm_track_points ADD COLUMN IF NOT EXISTS {col} {typ}"
            ))
        conn.commit()

    # storm_satellite_images is created via create_all above (new table)

    logger.info("Database schema ready.")
    _seed_lightning_sources()
    logger.info("Lightning sources seeded.")

    # ── Storm crawlers ────────────────────────────────────────────────────────
    scheduler.add_job(
        nhc_worker.run_nhc_crawler,
        "interval", minutes=15, id="nhc_crawler", max_instances=1, coalesce=True,
    )
    scheduler.add_job(
        jma_worker.run_jma_crawler,
        "interval", minutes=20, id="jma_crawler", max_instances=1, coalesce=True,
    )
    scheduler.add_job(
        jtwc_worker.run_jtwc_crawler,
        "interval", minutes=30, id="jtwc_crawler", max_instances=1, coalesce=True,
    )
    scheduler.add_job(
        ibtracs_worker.run_ibtracs_crawler,
        "cron", hour=2, minute=0, id="ibtracs_import", max_instances=1, coalesce=True,
    )

    # ── Lightning crawlers ────────────────────────────────────────────────────
    # GOES GLM: every 3 minutes (near-realtime LCFA files)
    scheduler.add_job(
        goes_glm_worker.run_goes_glm_crawler,
        "interval", minutes=3, id="goes_glm_crawler", max_instances=1, coalesce=True,
    )

    # GFS Risk: every 30 minutes (new GFS runs every 6 hours, poll to detect)
    scheduler.add_job(
        gfs_risk_worker.run_gfs_risk_crawler,
        "interval", minutes=30, id="gfs_risk_crawler", max_instances=1, coalesce=True,
    )

    # MTG LI: every 10 minutes if credentials configured, else no-op
    scheduler.add_job(
        mtg_li_worker.run_mtg_li_crawler,
        "interval", minutes=10, id="mtg_li_crawler", max_instances=1, coalesce=True,
    )

    # Daily event cleanup (retention 30 days)
    scheduler.add_job(
        goes_glm_worker._delete_old_events,
        "cron", hour=3, minute=30, id="lightning_cleanup", max_instances=1, coalesce=True,
    )

    # NASA LIS: weekly historical batch (Sunday 04:00 UTC)
    scheduler.add_job(
        nasa_lis_worker.run_nasa_lis_import,
        "cron", day_of_week="sun", hour=4, minute=0,
        id="nasa_lis_import", max_instances=1, coalesce=True,
    )

    scheduler.start()
    logger.info("Scheduler started.")

    # Run P0 crawlers once at startup
    asyncio.get_event_loop().run_in_executor(None, nhc_worker.run_nhc_crawler)
    asyncio.get_event_loop().run_in_executor(None, jma_worker.run_jma_crawler)

    # Run GFS risk and GOES GLM once at startup to populate initial data
    asyncio.get_event_loop().run_in_executor(None, gfs_risk_worker.run_gfs_risk_crawler)
    asyncio.get_event_loop().run_in_executor(None, goes_glm_worker.run_goes_glm_crawler)

    logger.info("Weather Tracking API v3 is ready.")


@app.on_event("shutdown")
async def shutdown_event():
    scheduler.shutdown(wait=False)
    logger.info("Scheduler stopped.")


# ── Root health ───────────────────────────────────────────────────────────────

@app.get("/", include_in_schema=False)
def root():
    return {"service": "weather-tracking", "version": "3.0.0", "modules": ["storms", "lightning"]}
