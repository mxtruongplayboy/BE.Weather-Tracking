import asyncio
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from core.database import Base, engine, init_postgis
from sqlalchemy import text
from models import storm_models  # noqa: F401 — registers all ORM models with Base

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Weather Tracking API",
    version="2.0.0",
    description=(
        "Global tropical cyclone tracking backend. "
        "Data sources: NOAA/NHC, JMA, JTWC, IBTrACS."
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

from api import admin, health, lightning, storms

app.include_router(storms.router)
app.include_router(storms.map_router)
app.include_router(health.router)
app.include_router(admin.router)
app.include_router(lightning.router)

# ── Scheduler ─────────────────────────────────────────────────────────────────

from crawlers import ibtracs_worker, jma_worker, jtwc_worker, lightning_worker, nhc_worker

scheduler = AsyncIOScheduler(timezone="UTC")


@app.on_event("startup")
async def startup_event():
    logger.info("Initializing database…")
    try:
        init_postgis(engine)
    except Exception as e:
        logger.warning(f"PostGIS init warning (may already be enabled): {e}")
    Base.metadata.create_all(bind=engine)
    # Add per-point movement columns if upgrading from older schema
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
    logger.info("Database schema ready.")

    # ── NHC (P0) — active every 15 min, forecast/cone every 30 min ──────────
    scheduler.add_job(
        nhc_worker.run_nhc_crawler,
        "interval",
        minutes=15,
        id="nhc_crawler",
        max_instances=1,
        coalesce=True,
    )

    # ── JMA (P0) — targetTc + TC detail every 20 min ────────────────────────
    scheduler.add_job(
        jma_worker.run_jma_crawler,
        "interval",
        minutes=20,
        id="jma_crawler",
        max_instances=1,
        coalesce=True,
    )

    # ── JTWC (P1) — ATCF b-deck every 30 min ────────────────────────────────
    scheduler.add_job(
        jtwc_worker.run_jtwc_crawler,
        "interval",
        minutes=30,
        id="jtwc_crawler",
        max_instances=1,
        coalesce=True,
    )

    # ── IBTrACS (P1) — daily historical import at 02:00 UTC ─────────────────
    scheduler.add_job(
        ibtracs_worker.run_ibtracs_crawler,
        "cron",
        hour=2,
        minute=0,
        id="ibtracs_import",
        max_instances=1,
        coalesce=True,
    )

    scheduler.start()
    logger.info("Scheduler started.")

    # Run P0 crawlers once at startup
    asyncio.get_event_loop().run_in_executor(None, nhc_worker.run_nhc_crawler)
    asyncio.get_event_loop().run_in_executor(None, jma_worker.run_jma_crawler)

    # Real-time lightning (mock / blitzortung)
    asyncio.create_task(lightning_worker.run_lightning_crawler())
    logger.info("Weather Tracking API is ready.")


@app.on_event("shutdown")
async def shutdown_event():
    scheduler.shutdown(wait=False)
    logger.info("Scheduler stopped.")


# ── Simple root health ────────────────────────────────────────────────────────

@app.get("/", include_in_schema=False)
def root():
    return {"service": "weather-tracking", "version": "2.0.0"}
