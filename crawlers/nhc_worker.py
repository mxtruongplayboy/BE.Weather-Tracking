import os
import requests
import zipfile
import io
import geopandas as gpd
import logging
import glob
from datetime import datetime

from core.database import SessionLocal
from models.storm import Storm

logger = logging.getLogger(__name__)

NHC_ACTIVE_STORMS_URL = "https://www.nhc.noaa.gov/CurrentStorms.json"
NHC_GIS_ARCHIVE_URL = "https://www.nhc.noaa.gov/gis/forecast/archive/{storm_id}_5day_latest.zip"

def fetch_active_storms():
    response = requests.get(NHC_ACTIVE_STORMS_URL, timeout=15)
    response.raise_for_status()
    data = response.json()
    return data.get("activeStorms", [])

def process_storm(storm_data, db):
    storm_id = storm_data["id"].lower() # e.g. "ep012026"
    name = storm_data.get("name", "Unknown")
    lat = storm_data.get("latitudeNumeric")
    lon = storm_data.get("longitudeNumeric")
    wind_max = storm_data.get("intensity")
    pressure = storm_data.get("pressure")
    
    basin = "Atlantic" if storm_id.startswith("al") else "East Pacific" if storm_id.startswith("ep") else "Central Pacific"
    
    storm = db.query(Storm).filter(Storm.id == storm_id).first()
    if not storm:
        storm = Storm(id=storm_id)
        db.add(storm)
    
    storm.name = name
    storm.basin = basin
    storm.is_active = True
    if lat is not None: storm.lat = float(lat)
    if lon is not None: storm.lon = float(lon)
    if wind_max is not None: storm.wind_max = float(wind_max)
    if pressure is not None: storm.pressure = float(pressure)
    storm.last_updated = datetime.utcnow()
    
    # Try fetching GIS data
    try:
        zip_url = NHC_GIS_ARCHIVE_URL.format(storm_id=storm_id)
        r = requests.get(zip_url, timeout=15)
        if r.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                extract_dir = f"/tmp/nhc_{storm_id}"
                os.makedirs(extract_dir, exist_ok=True)
                z.extractall(extract_dir)
                
                # Polygon: Cone of uncertainty
                pgn_files = glob.glob(f"{extract_dir}/*_5day_pgn.shp")
                if pgn_files:
                    gdf = gpd.read_file(pgn_files[0])
                    if gdf.crs != "EPSG:4326":
                        gdf = gdf.to_crs("EPSG:4326")
                    storm.cone_geojson = gdf.to_json()
                    
                # Points: Forecast track
                pts_files = glob.glob(f"{extract_dir}/*_5day_pts.shp")
                if pts_files:
                    gdf = gpd.read_file(pts_files[0])
                    if gdf.crs != "EPSG:4326":
                        gdf = gdf.to_crs("EPSG:4326")
                    storm.forecast_track_geojson = gdf.to_json()
                
                # Cleanup
                for f in glob.glob(f"{extract_dir}/*"):
                    os.remove(f)
                os.rmdir(extract_dir)
    except Exception as e:
        logger.error(f"Failed to fetch GIS data for {storm_id}: {e}")
        
    db.commit()

def run_nhc_crawler():
    logger.info("Started NHC Storm Crawler")
    db = SessionLocal()
    try:
        storms = fetch_active_storms()
        db.query(Storm).filter(Storm.basin.in_(["Atlantic", "East Pacific", "Central Pacific"])).update({"is_active": False})
        
        for s in storms:
            process_storm(s, db)
            
        logger.info(f"Successfully processed {len(storms)} NHC storms.")
    except Exception as e:
        logger.error(f"NHC Crawler Error: {e}")
    finally:
        db.close()
