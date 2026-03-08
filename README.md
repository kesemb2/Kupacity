# Israel Cinema Box Office Dashboard

דאשבורד בוקס אופיס לבתי קולנוע בישראל - מערכת שאוספת נתוני הקרנות מרשתות קולנוע ומציגה אותם בדאשבורד אינטראקטיבי.

## Features

- **Scrapers** for Israeli cinema chains: Cinema City / Yes Planet, Hot Cinema, Lev Cinema, Globus Max
- **Dashboard** with revenue analytics, top movies, occupancy rates
- **Movie detail** view with per-cinema and per-date breakdown
- **City analysis** showing revenue distribution across Israel
- **Auto-refresh** - scrapers run every 30 minutes

## Architecture

- **Backend**: Python / FastAPI + SQLAlchemy + SQLite
- **Frontend**: React + Recharts
- **Scrapers**: httpx + BeautifulSoup

## Quick Start

### Backend

```bash
cd backend
pip install -r requirements.txt
python main.py
```

The API runs on http://localhost:8000. Demo data is seeded automatically.

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
| `GET /api/dashboard/summary` | Overall dashboard stats |
| `GET /api/movies` | All movies with revenue summary |
| `GET /api/movies/:id` | Movie detail with per-cinema breakdown |
| `GET /api/cinemas` | All cinemas with stats |
| `GET /api/cities` | Revenue by city |
| `GET /api/chains` | Revenue by cinema chain |
| `GET /api/analytics/revenue-by-date` | Daily revenue trend |
| `GET /api/analytics/top-movies` | Top movies by revenue |
| `GET /api/analytics/occupancy-by-format` | Occupancy by screening format |
