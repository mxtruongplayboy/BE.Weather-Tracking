from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
import json

from core.database import get_db
from models import storm, schemas

router = APIRouter(prefix="/api/v1/storms", tags=["storms"])

@router.get("/active", response_model=List[schemas.StormBase])
def get_active_storms(db: Session = Depends(get_db)):
    active_storms = db.query(storm.Storm).filter(storm.Storm.is_active == True).all()
    results = []
    for s in active_storms:
        results.append(schemas.StormBase(
            id=s.id,
            name=s.name,
            basin=s.basin,
            is_active=s.is_active,
            current=schemas.CurrentStatus(
                lat=s.lat,
                lon=s.lon,
                wind_max=s.wind_max,
                pressure=s.pressure
            )
        ))
    return results

@router.get("/{storm_id}", response_model=schemas.StormDetail)
def get_storm_detail(storm_id: str, db: Session = Depends(get_db)):
    s = db.query(storm.Storm).filter(storm.Storm.id == storm_id).first()
    if not s:
        raise HTTPException(status_code=404, detail="Storm not found")
        
    return schemas.StormDetail(
        id=s.id,
        name=s.name,
        basin=s.basin,
        is_active=s.is_active,
        current=schemas.CurrentStatus(
            lat=s.lat,
            lon=s.lon,
            wind_max=s.wind_max,
            pressure=s.pressure
        ),
        past_track_geojson=json.loads(s.past_track_geojson) if s.past_track_geojson else None,
        forecast_track_geojson=json.loads(s.forecast_track_geojson) if s.forecast_track_geojson else None,
        cone_geojson=json.loads(s.cone_geojson) if s.cone_geojson else None,
        last_updated=s.last_updated
    )
