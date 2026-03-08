import sys
import os
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# Ensure backend directory is in path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import engine, Base, SessionLocal
from api.routes import router
from seed_data import seed_database

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: create tables and seed data
    Base.metadata.create_all(bind=engine)
    seed_database()
    logger.info("Database initialized and seeded")

    # Optionally start the scheduler for periodic scraping
    try:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        from scrapers.manager import run_all_scrapers

        scheduler = AsyncIOScheduler()

        async def scheduled_scrape():
            db = SessionLocal()
            try:
                await run_all_scrapers(db)
            finally:
                db.close()

        # Run scraper every 30 minutes
        scheduler.add_job(scheduled_scrape, "interval", minutes=30)
        scheduler.start()
        logger.info("Scraper scheduler started (every 30 min)")
    except Exception as e:
        logger.warning(f"Scheduler not started: {e}")

    yield


app = FastAPI(
    title="Israel Cinema Box Office Dashboard",
    description="דאשבורד בוקס אופיס לבתי קולנוע בישראל",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)

# Serve frontend build if it exists
frontend_build = os.path.join(os.path.dirname(__file__), "..", "frontend", "build")
if os.path.exists(frontend_build):
    app.mount("/", StaticFiles(directory=frontend_build, html=True), name="frontend")


@app.get("/api/health")
def health():
    return {"status": "ok", "service": "Israel Cinema Box Office Dashboard"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
