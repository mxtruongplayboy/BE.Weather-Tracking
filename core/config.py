import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql://weather:weather_secret@localhost:5432/weather_tracking"
    redis_url: str = "redis://localhost:6379/0"
    admin_api_token: str = "change_me_in_production"
    raw_data_dir: str = "./raw_data"

    # Cache TTL seconds
    cache_ttl_active_storms: int = 300        # 5 min
    cache_ttl_storm_detail: int = 600         # 10 min
    cache_ttl_track: int = 1800              # 30 min
    cache_ttl_forecast: int = 1800           # 30 min
    cache_ttl_cone: int = 1800              # 30 min
    cache_ttl_geojson: int = 1800           # 30 min

    # Stale threshold minutes — data older than this is flagged isStale
    stale_threshold_minutes: int = 90

    # Disk retention. Crawlers save every fetched payload under raw_data_dir
    # (GOES alone writes ~960 files/day); without a purge the volume grew to
    # 18 GB and filled the VPS disk, taking every service down with it.
    raw_retention_days: int = 3
    sync_log_retention_days: int = 14
    # GFS risk runs to keep in DB (APIs only ever read the latest one).
    gfs_risk_keep_runs: int = 2

    # Lightning module TTLs (seconds)
    cache_ttl_lightning_events: int = 90       # recent strikes — near-realtime
    cache_ttl_lightning_risk: int = 1800       # GFS risk — 30 min
    cache_ttl_lightning_tile: int = 900        # PNG risk tile — 15 min
    cache_ttl_hazards_summary: int = 180       # unified summary — 3 min

    # EUMETSAT credentials (optional; set to enable MTG LI ingestion)
    eumetsat_consumer_key: str = ""
    eumetsat_consumer_secret: str = ""

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
