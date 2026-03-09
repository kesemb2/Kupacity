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

    # Start scheduler for periodic scraping with different intervals
    try:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        from scrapers.manager import (
            run_all_scrapers,
            hot_cinema_weekly_movies,
            hot_cinema_daily_screenings,
            hot_cinema_update_tickets,
            close_expired_screenings,
        )

        scheduler = AsyncIOScheduler()

        # --- General scrape for all chains (every 30 min) ---
        async def scheduled_scrape():
            db = SessionLocal()
            try:
                await run_all_scrapers(db)
            finally:
                db.close()

        scheduler.add_job(scheduled_scrape, "interval", minutes=30,
                          id="all_scrapers")

        # --- Hot Cinema: weekly movie catalog refresh (every Sunday at 03:00) ---
        async def scheduled_hot_weekly():
            db = SessionLocal()
            try:
                await hot_cinema_weekly_movies(db)
            finally:
                db.close()

        scheduler.add_job(scheduled_hot_weekly, "cron", day_of_week="sun",
                          hour=3, minute=0, id="hot_weekly_movies")

        # --- Hot Cinema: daily screenings refresh (every day at 06:00) ---
        async def scheduled_hot_daily():
            db = SessionLocal()
            try:
                await hot_cinema_daily_screenings(db)
            finally:
                db.close()

        scheduler.add_job(scheduled_hot_daily, "cron", hour=6, minute=0,
                          id="hot_daily_screenings")

        # --- Hot Cinema: ticket count updates (every 5 hours) ---
        async def scheduled_hot_tickets():
            db = SessionLocal()
            try:
                await hot_cinema_update_tickets(db)
            finally:
                db.close()

        scheduler.add_job(scheduled_hot_tickets, "interval", hours=5,
                          id="hot_ticket_updates")

        # --- Close expired screenings (every minute) ---
        def scheduled_close_expired():
            db = SessionLocal()
            try:
                close_expired_screenings(db)
            finally:
                db.close()

        scheduler.add_job(scheduled_close_expired, "interval", minutes=1,
                          id="close_expired_screenings")

        scheduler.start()
        logger.info("Scheduler started: all_scrapers(30m), hot_weekly(Sun 03:00), "
                     "hot_daily(06:00), hot_tickets(5h), close_expired(1m)")
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
