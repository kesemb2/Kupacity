import sys
import os
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# Ensure backend directory is in path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import engine, Base, SessionLocal
from api.routes import router

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Silence noisy polling endpoints in uvicorn access log
# ---------------------------------------------------------------------------
class _QuietPollFilter(logging.Filter):
    """Drop access-log lines for high-frequency polling endpoints."""
    _NOISY = ("/api/scrape-logs",)

    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        return not any(ep in msg for ep in self._NOISY)


logging.getLogger("uvicorn.access").addFilter(_QuietPollFilter())

# Scraper proxy – set these env vars on your hosting provider (e.g. Render)
# to route SeleniumBase UC mode traffic through a residential/rotating proxy:
#   SCRAPER_PROXY_SERVER   = "http://proxy-host:port"
#   SCRAPER_PROXY_USERNAME = "user"        (optional)
#   SCRAPER_PROXY_PASSWORD = "password"    (optional)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: create tables and seed if empty
    Base.metadata.create_all(bind=engine)

    # Migrate: add blocked_seats_excluded column if missing
    try:
        with engine.connect() as conn:
            conn.execute(
                __import__("sqlalchemy").text(
                    "ALTER TABLE screenings ADD COLUMN blocked_seats_excluded INTEGER DEFAULT 0"
                )
            )
            conn.commit()
    except Exception:
        pass  # column already exists

    from models.models import CinemaChain
    db = SessionLocal()
    try:
        from seed_data import seed_database
        # Seed any missing chains (idempotent - skips existing)
        seed_database()
        chain_count = db.query(CinemaChain).count()
        logger.info(f"{chain_count} cinema chain(s) in database")
    except Exception as e:
        logger.warning(f"Seed data loading failed: {e}")
    finally:
        db.close()

    # Start scheduler for Hot Cinema scraping
    try:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        from scrapers.manager import (
            hot_cinema_weekly_movies,
            hot_cinema_daily_screenings,
            hot_cinema_update_tickets,
            movieland_weekly_movies,
            movieland_daily_screenings,
            movieland_update_tickets,
            close_expired_screenings,
            run_initial_scrape,
            run_movieland_initial_scrape,
        )

        scheduler = AsyncIOScheduler()

        # --- Hot Cinema: weekly movie catalog refresh (every Sunday at 03:00) ---
        async def scheduled_hot_weekly():
            db = SessionLocal()
            try:
                await hot_cinema_weekly_movies(db)
            finally:
                db.close()

        scheduler.add_job(scheduled_hot_weekly, "cron", day_of_week="sun",
                          hour=3, minute=0, id="hot_weekly_movies",
                          max_instances=1, coalesce=True)

        # --- Hot Cinema: daily screenings refresh (every day at 06:00) ---
        async def scheduled_hot_daily():
            db = SessionLocal()
            try:
                await hot_cinema_daily_screenings(db)
            finally:
                db.close()

        scheduler.add_job(scheduled_hot_daily, "cron", hour=6, minute=0,
                          id="hot_daily_screenings",
                          max_instances=1, coalesce=True)

        # --- Hot Cinema: ticket count updates (every 5 hours) ---
        async def scheduled_hot_tickets():
            db = SessionLocal()
            try:
                await hot_cinema_update_tickets(db)
            finally:
                db.close()

        scheduler.add_job(scheduled_hot_tickets, "interval", hours=5,
                          id="hot_ticket_updates",
                          max_instances=1, coalesce=True)

        # --- Movieland: weekly movie catalog refresh (every Sunday at 04:00) ---
        async def scheduled_mvl_weekly():
            db = SessionLocal()
            try:
                await movieland_weekly_movies(db)
            finally:
                db.close()

        scheduler.add_job(scheduled_mvl_weekly, "cron", day_of_week="sun",
                          hour=4, minute=0, id="mvl_weekly_movies",
                          max_instances=1, coalesce=True)

        # --- Movieland: daily screenings refresh (every day at 07:00) ---
        async def scheduled_mvl_daily():
            db = SessionLocal()
            try:
                await movieland_daily_screenings(db)
            finally:
                db.close()

        scheduler.add_job(scheduled_mvl_daily, "cron", hour=7, minute=0,
                          id="mvl_daily_screenings",
                          max_instances=1, coalesce=True)

        # --- Movieland: ticket count updates (every 5 hours, offset by 2.5h) ---
        async def scheduled_mvl_tickets():
            db = SessionLocal()
            try:
                await movieland_update_tickets(db)
            finally:
                db.close()

        scheduler.add_job(scheduled_mvl_tickets, "interval", hours=5,
                          start_date="2026-01-01 02:30:00",
                          id="mvl_ticket_updates",
                          max_instances=1, coalesce=True)

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
        logger.info("Scheduler started: hot_weekly(Sun 03:00), hot_daily(06:00), hot_tickets(5h), "
                     "mvl_weekly(Sun 04:00), mvl_daily(07:00), mvl_tickets(5h+2.5h), close_expired(1m)")

        # Run initial scrape if DB is empty — sequentially to avoid SQLite locking
        async def initial_scrape():
            from models.models import Movie
            db_check = SessionLocal()
            try:
                if db_check.query(Movie).count() == 0:
                    logger.info("Database empty - running initial scrape for both chains sequentially...")
                    db_hot = SessionLocal()
                    try:
                        await run_initial_scrape(db_hot)
                    except Exception:
                        logger.exception("Hot Cinema initial scrape failed")
                    finally:
                        db_hot.close()

                    db_mvl = SessionLocal()
                    try:
                        await run_movieland_initial_scrape(db_mvl)
                    except Exception:
                        logger.exception("Movieland initial scrape failed")
                    finally:
                        db_mvl.close()
            finally:
                db_check.close()

        import asyncio
        asyncio.create_task(initial_scrape())

    except Exception as e:
        logger.warning(f"Scheduler not started: {e}")

    yield


app = FastAPI(
    title="Kupacity - Israel Cinema Seat Analytics",
    description="כל הכיסאות בפריים אחד — דאשבורד נתוני מושבים לבתי קולנוע בישראל",
    version="1.2.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/health")
def health():
    return {"status": "ok", "service": "Kupacity"}


@app.get("/")
def root():
    return {"status": "ok", "service": "Kupacity", "docs": "/docs"}


app.include_router(router)

# Debug endpoint: view the last scraper screenshot
# Visit https://<your-render-url>/api/debug-screenshot in your browser
_DEBUG_SCREENSHOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "debug.png")


@app.get("/api/debug-screenshot")
def debug_screenshot():
    if os.path.exists(_DEBUG_SCREENSHOT):
        return FileResponse(_DEBUG_SCREENSHOT, media_type="image/png")
    return JSONResponse(
        status_code=404,
        content={"detail": "No debug screenshot yet. Run a scrape first."},
    )


_TICKET_DEBUG_SCREENSHOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "debug_tickets.png")


@app.get("/api/debug-screenshot-tickets")
def debug_screenshot_tickets():
    if os.path.exists(_TICKET_DEBUG_SCREENSHOT):
        return FileResponse(_TICKET_DEBUG_SCREENSHOT, media_type="image/png")
    return JSONResponse(
        status_code=404,
        content={"detail": "No ticket debug screenshot yet. Run a scrape with ticket updates first."},
    )

# Debug screenshots gallery
_DEBUG_SCREENSHOTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "debug_screenshots")


@app.get("/api/debug-screenshots")
def list_debug_screenshots():
    """List all debug screenshots with metadata."""
    os.makedirs(_DEBUG_SCREENSHOTS_DIR, exist_ok=True)
    files = []
    for f in sorted(os.listdir(_DEBUG_SCREENSHOTS_DIR), reverse=True):
        if not f.endswith(".png"):
            continue
        path = os.path.join(_DEBUG_SCREENSHOTS_DIR, f)
        stat = os.stat(path)
        files.append({
            "filename": f,
            "size_kb": round(stat.st_size / 1024, 1),
            "created_at": stat.st_mtime,
        })
    return files


@app.get("/api/debug-screenshots/{filename}")
def get_debug_screenshot(filename: str):
    """Serve a specific debug screenshot."""
    # Prevent path traversal
    if "/" in filename or "\\" in filename or ".." in filename:
        return JSONResponse(status_code=400, content={"detail": "Invalid filename"})
    path = os.path.join(_DEBUG_SCREENSHOTS_DIR, filename)
    if os.path.exists(path):
        return FileResponse(path, media_type="image/png")
    return JSONResponse(status_code=404, content={"detail": "Screenshot not found"})


@app.delete("/api/debug-screenshots")
def clear_debug_screenshots():
    """Delete all debug screenshots."""
    os.makedirs(_DEBUG_SCREENSHOTS_DIR, exist_ok=True)
    count = 0
    for f in os.listdir(_DEBUG_SCREENSHOTS_DIR):
        if f.endswith(".png"):
            os.remove(os.path.join(_DEBUG_SCREENSHOTS_DIR, f))
            count += 1
    return {"deleted": count}


# Serve frontend build if it exists
frontend_build = os.path.join(os.path.dirname(__file__), "..", "frontend", "build")
if os.path.exists(frontend_build):
    app.mount("/", StaticFiles(directory=frontend_build, html=True), name="frontend")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
