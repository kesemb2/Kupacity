# Kupacity

> כל הכיסאות בפריים אחד

**Israel Cinema Seat Analytics** — מערכת שאוספת נתוני מושבים והקרנות מרשתות קולנוע בישראל בזמן אמת ומציגה אותם בדאשבורד אינטראקטיבי.

## Features

- **Playwright-based scraper** for Hot Cinema - navigates Angular SPA booking pages, parses SVG seat maps to count sold/available seats in real-time
- **Self-learning blocked seats** - automatically detects permanently blocked seats per hall by tracking sold-seat positions across scrape runs (intersection-based, 90% threshold over 3+ runs)
- **Incremental data saves** - dashboard updates in real-time during long scrapes via callback pattern
- **Dashboard** with ticket analytics, top movies, occupancy rates, blocked seats stats
- **Movie detail** view with per-cinema and per-date breakdown, individual screening seat data
- **City analysis** showing ticket distribution across Israel
- **Auto-refresh** - ticket scrapes every 5 hours, daily screening refresh, weekly movie catalog refresh
- **Anti-overlap scheduling** - APScheduler with `max_instances=1, coalesce=True` prevents concurrent scrapes

## Architecture

- **Backend**: Python / FastAPI + SQLAlchemy + SQLite (WAL mode)
- **Frontend**: React 18 + Recharts
- **Scraper**: Playwright (headless Chromium) for browser automation
- **Scheduler**: APScheduler (BackgroundScheduler) for periodic scrapes

## Data Models

| Model | Purpose |
|---|---|
| `CinemaChain` | Cinema chain (Hot Cinema, etc.) |
| `Cinema` | Individual branch with city info |
| `Movie` | Movie metadata (title, genre, director, rating) |
| `Screening` | Specific screening with showtime, hall, format, tickets_sold, total_seats, blocked_seats_excluded |
| `HallSeatStats` | Per-hall seat frequency tracking for blocked seat learning |
| `ScrapeLog` | Scrape run history with progress tracking |

## Scraper Pipeline

1. **Weekly**: Full movie catalog refresh from Hot Cinema website
2. **Daily**: Screening schedule refresh (next 7 days) for all branches
3. **Every 5h**: Ticket count updates - opens each screening's booking page, parses SVG seat map:
   - Detects seat elements via image href (`/available/` vs `/unavailable/`) and CSS classes
   - Deduplicates seats using position keys: `Math.round(x/5), Math.round(y/5)`
   - Handles SVG-specific quirks (`SVGAnimatedString` for className, inherited fill colors)
   - Two parallel detection methods (color/image-based + class/attribute-based) with best-result selection
4. **Post-scrape**: Blocked seats finalization - computes intersection of sold positions per hall, updates `HallSeatStats`

## Quick Start

### Backend

```bash
cd backend
pip install -r requirements.txt
python main.py
```

The API runs on http://localhost:8000. Initial scrape starts automatically.

### Frontend

```bash
cd frontend
npm install
npm start
```

The dashboard runs on http://localhost:3000.

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /api/dashboard/summary` | Overall stats: movies, cinemas, screenings, tickets, blocked seats |
| `GET /api/movies` | All movies with ticket summary and avg occupancy |
| `GET /api/movies/:id` | Movie detail with per-cinema breakdown, daily tickets, individual screenings (includes blocked_seats_excluded, tickets_sold_raw) |
| `GET /api/cinemas` | All cinemas with stats |
| `GET /api/cities` | Tickets by city |
| `GET /api/analytics/tickets-by-date` | Daily ticket trend (configurable days) |
| `GET /api/analytics/tickets-by-branch` | Tickets per branch |
| `GET /api/analytics/top-movies` | Top movies by tickets (configurable limit) |
| `GET /api/analytics/occupancy-by-format` | Avg occupancy by format (2D, 3D, IMAX, etc.) |
| `POST /api/scrape/trigger` | Trigger manual scrape (runs in background) |
| `GET /api/scrape-logs` | Recent scrape history with progress |

## Key Technical Details

- **SVG seat detection**: Hot Cinema uses `<image href>` inside nested SVGs for seats. Detection prioritizes image URL (`/unavailable/` = sold, `/available/` = available), then CSS classes, then color analysis
- **SVGAnimatedString handling**: SVG elements return `SVGAnimatedString` from `el.className` - code uses `el.className.baseVal` instead of `.toString()`
- **Legend filtering**: Seat map may include legend elements - filtered by Y-position gap threshold (150px)
- **Row number filtering**: Text elements matching `/^\d{1,2}$/` with no children are excluded from seat count
- **Blocked seats learning**: Per (cinema, hall), tracks which seat positions are sold across runs. After 3+ runs, seats sold in ≥90% of runs (and in ALL screenings per run via intersection) are classified as permanently blocked
