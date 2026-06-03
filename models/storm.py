from sqlalchemy import Column, String, Float, Boolean, DateTime
from core.database import Base
from datetime import datetime

class Storm(Base):
    __tablename__ = "storms"

    id = Column(String, primary_key=True, index=True) # e.g. AL092024
    name = Column(String, index=True)
    basin = Column(String, index=True)
    is_active = Column(Boolean, default=True)
    
    # Current status
    lat = Column(Float, nullable=True)
    lon = Column(Float, nullable=True)
    wind_max = Column(Float, nullable=True) # in knots or m/s
    pressure = Column(Float, nullable=True) # in hPa
    
    # Storing GeoJSON as string for simplicity in SQLite, 
    # ideally we'd use PostGIS/JSONB in Postgres
    past_track_geojson = Column(String, nullable=True)
    forecast_track_geojson = Column(String, nullable=True)
    cone_geojson = Column(String, nullable=True)
    
    last_updated = Column(DateTime, default=datetime.utcnow)
