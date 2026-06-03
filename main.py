from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from core.database import Base, engine
from api import storms, lightning

# Create tables
Base.metadata.create_all(bind=engine)

app = FastAPI(title="BE.Weather-Tracking API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
)

from api import storms, lightning, admin
app.include_router(storms.router)
app.include_router(lightning.router)
app.include_router(admin.router)

from crawlers import nhc_worker, jtwc_worker, lightning_worker
import asyncio

scheduler = AsyncIOScheduler()

@app.on_event("startup")
async def startup_event():
    print("Starting Weather Tracking Workers...")
    
    # Run once at startup (using add_job with immediate execution or just call sync via run_in_executor)
    # We will schedule them to run every 30 minutes
    scheduler.add_job(nhc_worker.run_nhc_crawler, 'interval', minutes=30, id='nhc_crawler')
    scheduler.add_job(jtwc_worker.run_jtwc_crawler, 'interval', minutes=30, id='jtwc_crawler')
    
    scheduler.start()
    
    # Start long-running lightning websocket connection
    asyncio.create_task(lightning_worker.run_lightning_crawler())

@app.on_event("shutdown")
async def shutdown_event():
    scheduler.shutdown()

@app.get("/health")
def health_check():
    return {"status": "ok", "service": "weather-tracking"}
