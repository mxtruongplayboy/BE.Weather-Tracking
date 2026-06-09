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

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
