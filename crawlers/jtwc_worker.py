import requests
import xml.etree.ElementTree as ET
from datetime import datetime
import logging
import json

from core.database import SessionLocal
from models.storm import Storm

logger = logging.getLogger(__name__)

GDACS_RSS_URL = "https://www.gdacs.org/xml/rss.xml"

def fetch_gdacs_storms():
    response = requests.get(GDACS_RSS_URL, timeout=15)
    response.raise_for_status()
    root = ET.fromstring(response.content)
    
    storms = []
    # GDACS RSS uses namespaces
    namespaces = {'gdacs': 'http://www.gdacs.org', 'geo': 'http://www.w3.org/2003/01/geo/wgs84_pos#'}
    
    for item in root.findall('.//item'):
        event_type = item.find('gdacs:eventtype', namespaces)
        if event_type is not None and event_type.text == 'TC': # Tropical Cyclone
            event_id = item.find('gdacs:eventid', namespaces).text
            name = item.find('gdacs:eventname', namespaces)
            name = name.text if name is not None else "Unknown"
            
            lat = item.find('geo:lat', namespaces)
            lon = item.find('geo:long', namespaces)
            
            storms.append({
                "id": f"gdacs_{event_id}",
                "name": name,
                "lat": float(lat.text) if lat is not None else None,
                "lon": float(lon.text) if lon is not None else None
            })
            
    return storms

def run_jtwc_crawler():
    """
    Sử dụng nguồn GDACS (tổng hợp từ JTWC và các trung tâm khác)
    cho các cơn bão ngoài khu vực Mỹ.
    """
    logger.info("Started JTWC/GDACS Storm Crawler")
    db = SessionLocal()
    try:
        storms = fetch_gdacs_storms()
        # Mark GDACS storms as inactive before updating
        db.query(Storm).filter(Storm.id.like("gdacs_%")).update({"is_active": False})
        
        for s in storms:
            storm = db.query(Storm).filter(Storm.id == s["id"]).first()
            if not storm:
                storm = Storm(id=s["id"])
                db.add(storm)
                
            storm.name = s["name"]
            storm.basin = "Global"
            storm.is_active = True
            if s["lat"] is not None: storm.lat = s["lat"]
            if s["lon"] is not None: storm.lon = s["lon"]
            storm.last_updated = datetime.utcnow()
            
            # TODO: Fetch detailed shapefile/GeoJSON for GDACS event if needed
            # For now, we only plot the current point.
            point_geojson = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [storm.lon, storm.lat]
                },
                "properties": {"name": storm.name}
            }
            storm.forecast_track_geojson = json.dumps(point_geojson)
            
        db.commit()
        logger.info(f"Successfully processed {len(storms)} JTWC/GDACS storms.")
    except Exception as e:
        logger.error(f"JTWC Crawler Error: {e}")
    finally:
        db.close()
