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
    allow_headers=["*"],
)

app.include_router(storms.router)
app.include_router(lightning.router)

scheduler = AsyncIOScheduler()

@app.on_event("startup")
async def startup_event():
    print("Starting Weather Tracking Workers...")
    # TODO: Add crawler jobs to scheduler here
    # scheduler.add_job(nhc_worker.run, 'interval', minutes=60)
    scheduler.start()

@app.on_event("shutdown")
async def shutdown_event():
    scheduler.shutdown()

@app.get("/health")
def health_check():
    return {"status": "ok", "service": "weather-tracking"}
