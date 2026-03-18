# Kupacity - Israel Cinema Seat Analytics

> כל הכיסאות בפריים אחד

Real-time cinema seat/ticket analytics for Israeli cinema chains. Scrapes screening schedules and seat maps, displays analytics in a Hebrew RTL dashboard.

## Tech Stack

- **Backend**: Python 3 / FastAPI 0.115.6 / SQLAlchemy 2.0.36 / SQLite (WAL mode)
- **Scraping**: Playwright headless Chromium (with playwright-stealth)
- **Scheduling**: APScheduler (AsyncIOScheduler)
- **Frontend**: React 18 / Recharts / Axios
- **Deploy**: Docker Compose (backend:8000, frontend:3000→80 via nginx)
- **Hosting**: Render (backend URL: cinema-back-kjkx.onrender.com)

## Commands

```bash
# Backend
cd backend && pip install -r requirements.txt && python main.py  # API on :8000

# Frontend
cd frontend && npm install && npm start  # Dashboard on :3000

# Docker
docker-compose up --build

# No tests exist - manual testing + scrape log monitoring
```

## Project Structure

```
cinema/
├── docker-compose.yml          # backend:8000 + frontend:3000→80
├── CLAUDE.md                   # This file
├── README.md                   # Project overview
│
├── backend/
│   ├── main.py                 # FastAPI app, lifespan, APScheduler config
│   ├── database.py             # SQLAlchemy engine, SessionLocal, SQLite WAL
│   ├── seed_data.py            # Seeds Hot Cinema chain + 10 branches on first run
│   ├── models/models.py        # 7 ORM models (see below)
│   ├── api/routes.py           # All REST API endpoints under /api/
│   └── scrapers/
│       ├── base.py             # BaseScraper ABC, ScrapedMovie, ScrapedScreening
│       ├── hot_cinema.py       # Hot Cinema scraper (Playwright, ~1800 lines)
│       ├── movieland.py        # Movieland scraper (Playwright, ~1600 lines)
│       ├── cinema_city.py      # Cinema City / Yes Planet (REST API)
│       ├── lev_cinema.py       # Lev Cinema (HTML parsing)
│       ├── globus_max.py       # Globus Max (REST API)
│       └── manager.py          # Orchestration: DB upserts, progress callbacks
│
└── frontend/
    └── src/
        ├── App.js              # Navigation, health check, layout (Hebrew RTL)
        ├── api/client.js       # Axios client, all API call functions
        ├── pages/
        │   ├── Dashboard.js    # 4 stat cards + 4 charts
        │   ├── MoviesPage.js   # Movie list with search/sort
        │   ├── MovieDetail.js  # Per-movie breakdown: charts + screening table
        │   ├── CinemasPage.js  # Cinema branches list
        │   ├── CitiesPage.js   # City-level analytics
        │   ├── AnalyticsPage.js # Advanced analytics
        │   └── ScrapePage.js   # Trigger scrape, progress bar, logs, debug screenshots
        └── components/
            ├── StatCard.js     # Metric display card
            ├── ChartCard.js    # Chart wrapper
            └── DataTable.js    # Generic sortable table (RTL)
```

## Database Models (backend/models/models.py)

| Model | Table | Purpose |
|---|---|---|
| `CinemaChain` | `cinema_chains` | רשת קולנוע (Hot Cinema, Movieland) |
| `Cinema` | `cinemas` | סניף ספציפי with city, chain FK |
| `Movie` | `movies` | סרט - title, genre, duration, poster. UNIQUE(title, release_date) |
| `Screening` | `screenings` | הקרנה - showtime, hall, format, language, tickets_sold, total_seats, blocked_seats_excluded, status (active/closed) |
| `TicketSnapshot` | `ticket_snapshots` | Historical ticket count snapshots per screening |
| `HallSeatStats` | `hall_seat_stats` | Per-hall seat frequency tracking for blocked seat learning. UNIQUE(cinema_id, hall) |
| `ScrapeLog` | `scrape_logs` | Scrape run log with progress JSON for live UI updates |

## Scraper Architecture

### Active Chains (scheduled in main.py)

**Hot Cinema** (הוט סינמה) - 10 branches:
- Modi'in, Kfar Saba, Petah Tikva, Rehovot, Haifa, Kiryon, Karmiel, Nahariya, Ashkelon, Ashdod
- URLs: `hotcinema.co.il/theater/{id}/{slug}`, `hotcinema.co.il/movie/{id}/{slug}`
- Ticket pages: `tickets.hotcinema.co.il/site/{id}`
- Screening discovery: Intercepts the `/tickets/movieevents` API (JSON) for each movie, iterating over 7 days via date parameter
- Seat counting: Playwright navigates to booking page SVG seat map, counts seats by image href (`/available/` vs `/unavailable/`) and color analysis

**Movieland** (מובילנד) - 5 branches:
- Tel Aviv, Netanya, Haifa, Karmiel, Afula
- URLs: `movieland.co.il/` with branch pages discovered via homepage links or fallback URLs
- Booking: `ecom.biggerpicture.ai` (BiggerPicture platform)
- Branch page structure: `.date-cont` containers hold movies, each with `a.bg-theater-c` (title), `.bg-genre` (genre/duration), `a[href*="/order/"]` (screening links with `eventID`)
- Seat counting: Image href matching `/mvl-seat/(available|unavailable)/` on BiggerPicture seat map pages
- Date navigation: URL-based (?date=) + JS date tab clicking. Branch URLs have fallback_urls in MOVIELAND_BRANCHES dict

### Inactive Scrapers (code exists but not scheduled)

- **Cinema City / Yes Planet**: REST API at `/il/data-api-service/v1/quickbook/...`
- **Lev Cinema**: HTML parsing with BeautifulSoup
- **Globus Max**: REST API at `/api/screenings/{branch_id}?date=...`

### Scraper Pipeline (3 phases)

```
Phase 1 - Weekly (Sun):    scrape_movies()           → movie catalog
Phase 2 - Daily:           scrape_screenings()       → screening schedule (7 days)
Phase 3 - Post-daily + 5h: scrape_ticket_updates()  → seat map counts
Phase 4 - Every 1 min:    close_expired_screenings() → mark past screenings "closed"
```

### Schedule (backend/main.py lifespan)

| Job ID | Schedule | Function | Notes |
|---|---|---|---|
| `hot_weekly_movies` | Sunday 03:00 | `hot_cinema_weekly_movies` | Full movie refresh |
| `hot_daily_screenings` | Daily 06:00 | `hot_cinema_daily_screenings` → `hot_cinema_update_tickets` | Chains ticket update after screenings |
| `hot_ticket_updates` | Every 5h | `hot_cinema_update_tickets` | Standalone backup interval |
| `mvl_weekly_movies` | Sunday 04:00 | `movieland_weekly_movies` | Full movie refresh |
| `mvl_daily_screenings` | Daily 07:00 | `movieland_daily_screenings` → `movieland_update_tickets` | Chains ticket update after screenings |
| `mvl_ticket_updates` | Every 5h (+2.5h offset) | `movieland_update_tickets` | Standalone backup interval |
| `close_expired_screenings` | Every 1 min | `close_expired_screenings` | Close screenings 10+ min past showtime |

### Manager (backend/scrapers/manager.py)

Key functions:
- `_get_or_create_chain/cinema/movie()` - Upsert helpers
- `_upsert_screenings(db, chain, screenings)` - Main upsert: creates cinema/movie if needed, updates screening only if `total_seats > 0`, creates `TicketSnapshot` on ticket count changes
- `_make_progress_callback(db, log)` - Returns callback that updates ScrapeLog.progress JSON in real-time
- `_make_screening_callback(db, chain, log)` - Returns per-screening update callback + hall_data accumulator for blocked seats
- `_finalize_blocked_seats(db, hall_data)` - Intersection-based blocked seat learning (90% threshold, 3+ scans)
- `run_initial_scrape(db)` / `run_movieland_initial_scrape(db)` - Startup scrape if DB empty (movies → screenings, skips tickets)

## API Endpoints (backend/api/routes.py)

All under `/api/`:

| Endpoint | Method | Description |
|---|---|---|
| `/dashboard/summary` | GET | Stats: movies, cinemas, screenings, tickets, top movie |
| `/movies` | GET | All movies with ticket counts, avg occupancy |
| `/movies/{id}` | GET | Movie detail: by_cinema, by_date, individual screenings |
| `/cinemas` | GET | All cinema branches with stats |
| `/cities` | GET | Tickets aggregated by city |
| `/analytics/tickets-by-date?days=14` | GET | Daily ticket trend (days=0 for all-time) |
| `/analytics/tickets-by-branch` | GET | Tickets per branch |
| `/analytics/top-movies?limit=10` | GET | Top movies by tickets |
| `/analytics/occupancy-by-format` | GET | Avg occupancy by format (2D/3D/IMAX/4DX) |
| `/analytics/tickets-by-hour` | GET | Ticket distribution by hour |
| `/analytics/occupancy-by-day-of-week` | GET | Occupancy by day of week |
| `/analytics/movie-trends` | GET | Movie sales trends |
| `/analytics/dead-screenings?threshold=10` | GET | Low-occupancy screenings |
| `/analytics/format-by-branch` | GET | Format distribution per branch |
| `/analytics/branch-efficiency` | GET | Branch efficiency metrics |
| `/analytics/genre-stats` | GET | Genre-level statistics |
| `/analytics/movie-lifetime/{id}` | GET | All-time movie stats |
| `/analytics/blocked-seats` | GET | Blocked seat learning stats per hall |
| `/scrape/trigger?chain=` | POST | Trigger manual scrape (background task) |
| `/scrape-logs?limit=20` | GET | Recent scrape logs with progress JSON |

Additional endpoints in main.py:
- `GET /api/health` - Health check
- `GET /api/debug-screenshot` - Last main scraper screenshot (debug.png)
- `GET /api/debug-screenshot-tickets` - Last ticket scraper screenshot
- `GET /api/debug-screenshots` - List all debug screenshots
- `GET /api/debug-screenshots/{filename}` - Serve specific screenshot
- `DELETE /api/debug-screenshots` - Clear all debug screenshots

## Key Technical Details

### Seat Map Parsing
- **Hot Cinema**: SVG seat map on `tickets.hotcinema.co.il`. Seats detected by `<image href>` containing `/available/` or `/unavailable/`. Deduplication via position keys `Math.round(x/5),Math.round(y/5)`. Handles `SVGAnimatedString` for className. Legend elements filtered by Y-position gap (150px). Two parallel methods (color + image based) with best-result selection.
- **Movieland**: BiggerPicture seat map on `ecom.biggerpicture.ai`. Seats detected by image href or background-image containing `/mvl-seat/(available|unavailable)/`. Seat types: regular, armchair (ac), love-seat (sl/sr), long love-seat (lsl/lsr), handicap.

### Blocked Seats Learning
Per (cinema_id, hall) in `HallSeatStats`:
1. Each scrape run records sold seat positions
2. `scan_count` incremented per run, `seat_sold_counts` JSON tracks per-position frequency
3. After 3+ scans, positions sold in ≥90% of runs are classified as permanently blocked
4. `blocked_seats` JSON array and `blocked_count` updated
5. `Screening.blocked_seats_excluded` tracks how many blocked seats were subtracted from tickets_sold

### Anti-Bot Measures
- playwright-stealth patches (navigator.webdriver, plugins, languages)
- Random human delays (0.5-1.5s between actions)
- Page scrolling simulation
- Realistic Chrome user agent + Hebrew locale
- Optional proxy support via `SCRAPER_PROXY_SERVER` env var

### Database
- SQLite with WAL mode, `check_same_thread=False`, timeout=30s
- Data stored in Docker volume `cinema-data` at `/app/data/cinema.db`
- Migration: `blocked_seats_excluded` column added via ALTER TABLE if missing (main.py startup)
- Anti-overlap: APScheduler `max_instances=1, coalesce=True` on all jobs

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `DATA_DIR` | `./data` | SQLite database directory |
| `SCRAPER_PROXY_SERVER` | (none) | Proxy URL for Playwright |
| `SCRAPER_PROXY_USERNAME` | (none) | Proxy auth username |
| `SCRAPER_PROXY_PASSWORD` | (none) | Proxy auth password |
| `REACT_APP_API_URL` | `https://cinema-back-kjkx.onrender.com` | Backend API base URL for frontend |

## Known Issues / Recent Changes

- Movieland branch discovery: Uses fallback URL probing (fast) instead of homepage scanning. Fallback URLs defined in `MOVIELAND_BRANCHES[bid]["fallback_urls"]`.
- Movieland date navigation: URL-based (`?date=`) + JS tab clicking. Old approach used `[class*="date"]` which matched `.date-cont` movie containers instead of date tabs.
- Ticket updates are chained after daily scraping (not just independent 5h intervals).
- Only Hot Cinema and Movieland are actively scheduled. Cinema City, Lev Cinema, Globus Max scrapers exist but are not integrated into the scheduler.
- No test suite exists.
- Frontend uses all inline styles with a dark theme and Heebo Hebrew font.
