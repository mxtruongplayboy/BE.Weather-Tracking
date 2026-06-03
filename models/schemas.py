from pydantic import BaseModel
from typing import Optional, Any
from datetime import datetime

class CurrentStatus(BaseModel):
    lat: Optional[float]
    lon: Optional[float]
    wind_max: Optional[float]
    pressure: Optional[float]

class StormBase(BaseModel):
    id: str
    name: str
    basin: str
    is_active: bool
    current: CurrentStatus
    
class StormDetail(StormBase):
    past_track_geojson: Optional[Any]
    forecast_track_geojson: Optional[Any]
    cone_geojson: Optional[Any]
    last_updated: datetime

    class Config:
        from_attributes = True
