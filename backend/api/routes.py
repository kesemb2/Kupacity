import asyncio
import logging

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, desc
from typing import Optional
from datetime import datetime, timedelta

from database import get_db, SessionLocal
from models.models import CinemaChain, Cinema, Movie, Screening, ScrapeLog, TicketSnapshot, HallSeatStats

router = APIRouter(prefix="/api")
logger = logging.getLogger(__name__)


# ─── Dashboard Summary ───────────────────────────────────────────────

@router.get("/dashboard/summary")
def get_dashboard_summary(db: Session = Depends(get_db)):
    """סיכום כללי לדאשבורד הראשי"""
    total_movies = db.query(Movie).count()
    total_cinemas = db.query(Cinema).count()
    total_screenings = db.query(Screening).count()
    total_tickets = db.query(func.sum(Screening.tickets_sold)).scalar() or 0
    total_blocked_excluded = db.query(func.sum(Screening.blocked_seats_excluded)).scalar() or 0

    # Top movie by tickets sold
    top_movie_row = (
        db.query(
            Movie.title, Movie.title_he,
            func.sum(Screening.tickets_sold).label("total_tickets"),
            func.count(Screening.id).label("total_screenings"),
        )
        .join(Screening)
        .group_by(Movie.id)
        .order_by(desc("total_tickets"))
        .first()
    )

    return {
        "total_movies": total_movies,
        "total_cinemas": total_cinemas,
        "total_screenings": total_screenings,
        "total_tickets_sold": total_tickets,
        "total_blocked_seats_excluded": total_blocked_excluded,
        "top_movie": {
            "title": top_movie_row[0] if top_movie_row else None,
            "title_he": top_movie_row[1] if top_movie_row else None,
            "tickets_sold": top_movie_row[2] if top_movie_row else 0,
            "screenings": top_movie_row[3] if top_movie_row else 0,
        } if top_movie_row else None,
    }


# ─── Movies ──────────────────────────────────────────────────────────

@router.get("/movies")
def get_movies(db: Session = Depends(get_db)):
    """רשימת כל הסרטים עם סיכום כרטיסים"""
    results = (
        db.query(
            Movie,
            func.count(Screening.id).label("screenings_count"),
            func.sum(Screening.tickets_sold).label("total_tickets"),
            func.avg(Screening.tickets_sold * 100.0 / func.nullif(Screening.total_seats, 0)).label("avg_occupancy"),
        )
        .outerjoin(Screening)
        .group_by(Movie.id)
        .order_by(desc("total_tickets"))
        .all()
    )

    return [
        {
            "id": movie.id,
            "title": movie.title,
            "title_he": movie.title_he,
            "genre": movie.genre,
            "duration_minutes": movie.duration_minutes,
            "release_date": movie.release_date,
            "poster_url": movie.poster_url,
            "rating": movie.rating,
            "director": movie.director,
            "screenings_count": screenings_count or 0,
            "total_tickets_sold": total_tickets or 0,
            "avg_occupancy": round(avg_occ or 0, 1),
        }
        for movie, screenings_count, total_tickets, avg_occ in results
    ]


@router.get("/movies/{movie_id}")
def get_movie_detail(movie_id: int, db: Session = Depends(get_db)):
    """פרטי סרט עם פירוט לפי בתי קולנוע"""
    movie = db.query(Movie).filter_by(id=movie_id).first()
    if not movie:
        return {"error": "Movie not found"}

    # Tickets by cinema
    by_cinema = (
        db.query(
            Cinema.name, Cinema.city,
            func.count(Screening.id).label("screenings"),
            func.sum(Screening.tickets_sold).label("tickets"),
        )
        .join(Screening, Screening.cinema_id == Cinema.id)
        .filter(Screening.movie_id == movie_id)
        .group_by(Cinema.id)
        .order_by(desc("tickets"))
        .all()
    )

    # Tickets by date
    by_date = (
        db.query(
            func.date(Screening.showtime).label("date"),
            func.sum(Screening.tickets_sold).label("tickets"),
            func.count(Screening.id).label("screenings"),
        )
        .filter(Screening.movie_id == movie_id)
        .group_by(func.date(Screening.showtime))
        .order_by("date")
        .all()
    )

    # Individual screenings with seat data
    screenings = (
        db.query(Screening, Cinema.name, Cinema.city)
        .join(Cinema, Screening.cinema_id == Cinema.id)
        .filter(Screening.movie_id == movie_id)
        .order_by(Screening.showtime)
        .all()
    )

    return {
        "movie": {
            "id": movie.id,
            "title": movie.title,
            "title_he": movie.title_he,
            "genre": movie.genre,
            "duration_minutes": movie.duration_minutes,
            "release_date": movie.release_date,
            "rating": movie.rating,
            "director": movie.director,
        },
        "by_cinema": [
            {
                "cinema": name,
                "city": city,
                "screenings": s,
                "tickets_sold": t,
            }
            for name, city, s, t in by_cinema
        ],
        "by_date": [
            {"date": str(d), "tickets_sold": t, "screenings": s}
            for d, t, s in by_date
        ],
        "screenings": [
            {
                "id": scr.id,
                "cinema": cname,
                "city": ccity,
                "showtime": scr.showtime.isoformat() if scr.showtime else None,
                "hall": scr.hall or "",
                "format": scr.format or "",
                "total_seats": scr.total_seats or 0,
                "tickets_sold": scr.tickets_sold or 0,
                "tickets_sold_raw": (scr.tickets_sold or 0) + (scr.blocked_seats_excluded or 0),
                "blocked_seats_excluded": scr.blocked_seats_excluded or 0,
                "occupancy": round((scr.tickets_sold or 0) * 100 / scr.total_seats, 1) if scr.total_seats else 0,
                "status": scr.status or "active",
            }
            for scr, cname, ccity in screenings
        ],
    }


# ─── Cinemas ─────────────────────────────────────────────────────────

@router.get("/cinemas")
def get_cinemas(db: Session = Depends(get_db)):
    """רשימת בתי קולנוע עם סיכום"""
    results = (
        db.query(
            Cinema,
            CinemaChain.name.label("chain_name"),
            CinemaChain.name_he.label("chain_name_he"),
            func.count(Screening.id).label("screenings_count"),
            func.sum(Screening.tickets_sold).label("total_tickets"),
        )
        .join(CinemaChain, Cinema.chain_id == CinemaChain.id)
        .outerjoin(Screening)
        .group_by(Cinema.id)
        .order_by(desc("total_tickets"))
        .all()
    )

    return [
        {
            "id": cinema.id,
            "name": cinema.name,
            "name_he": cinema.name_he,
            "city": cinema.city,
            "city_he": cinema.city_he,
            "chain": chain_name,
            "chain_he": chain_he,
            "halls_count": cinema.halls_count,
            "screenings_count": sc or 0,
            "total_tickets_sold": tt or 0,
        }
        for cinema, chain_name, chain_he, sc, tt in results
    ]


# ─── Cities ──────────────────────────────────────────────────────────

@router.get("/cities")
def get_cities(db: Session = Depends(get_db)):
    """סיכום לפי ערים"""
    results = (
        db.query(
            Cinema.city,
            Cinema.city_he,
            func.count(func.distinct(Cinema.id)).label("cinemas_count"),
            func.count(Screening.id).label("screenings_count"),
            func.sum(Screening.tickets_sold).label("total_tickets"),
        )
        .outerjoin(Screening)
        .group_by(Cinema.city)
        .order_by(desc("total_tickets"))
        .all()
    )

    return [
        {
            "city": city,
            "city_he": city_he,
            "cinemas_count": cc,
            "screenings_count": sc or 0,
            "total_tickets_sold": tt or 0,
        }
        for city, city_he, cc, sc, tt in results
    ]


# ─── Analytics ────────────────────────────────────────────────────────

@router.get("/analytics/tickets-by-date")
def get_tickets_by_date(days: int = Query(default=14), db: Session = Depends(get_db)):
    """כרטיסים לפי תאריך. days=0 מחזיר את כל ההיסטוריה"""
    query = db.query(
        func.date(Screening.showtime).label("date"),
        func.sum(Screening.tickets_sold).label("tickets"),
        func.count(Screening.id).label("screenings"),
    )
    if days > 0:
        cutoff = datetime.now() - timedelta(days=days)
        query = query.filter(Screening.showtime >= cutoff)
    results = (
        query
        .group_by(func.date(Screening.showtime))
        .order_by("date")
        .all()
    )

    return [
        {
            "date": str(d),
            "tickets_sold": t,
            "screenings_count": s,
        }
        for d, t, s in results
    ]


@router.get("/analytics/tickets-by-branch")
def get_tickets_by_branch(db: Session = Depends(get_db)):
    """כרטיסים לפי סניף הוט סינמה"""
    results = (
        db.query(
            Cinema.name,
            Cinema.city,
            func.sum(Screening.tickets_sold).label("tickets"),
            func.count(Screening.id).label("screenings"),
        )
        .join(Screening, Screening.cinema_id == Cinema.id)
        .group_by(Cinema.id)
        .order_by(desc("tickets"))
        .all()
    )

    return [
        {"name": n, "city": c, "tickets_sold": t, "screenings_count": s}
        for n, c, t, s in results
    ]


@router.get("/analytics/top-movies")
def get_top_movies(limit: int = Query(default=10), db: Session = Depends(get_db)):
    """הסרטים המובילים לפי כרטיסים"""
    results = (
        db.query(
            Movie.title,
            Movie.title_he,
            Movie.genre,
            func.sum(Screening.tickets_sold).label("total_tickets"),
            func.count(Screening.id).label("screenings"),
        )
        .join(Screening)
        .group_by(Movie.id)
        .order_by(desc("total_tickets"))
        .limit(limit)
        .all()
    )

    return [
        {
            "title": t, "title_he": th, "genre": g,
            "total_tickets_sold": tt,
            "screenings_count": s,
        }
        for t, th, g, tt, s in results
    ]


@router.get("/analytics/occupancy-by-format")
def get_occupancy_by_format(db: Session = Depends(get_db)):
    """תפוסה לפי פורמט הקרנה"""
    results = (
        db.query(
            Screening.format,
            func.count(Screening.id).label("screenings"),
            func.avg(Screening.tickets_sold * 100.0 / func.nullif(Screening.total_seats, 0)).label("avg_occupancy"),
            func.sum(Screening.tickets_sold).label("total_tickets"),
        )
        .group_by(Screening.format)
        .order_by(desc("total_tickets"))
        .all()
    )

    return [
        {
            "format": f,
            "screenings_count": s,
            "avg_occupancy": round(o or 0, 1),
            "total_tickets": t or 0,
        }
        for f, s, o, t in results
    ]


@router.get("/analytics/tickets-by-hour")
def get_tickets_by_hour(db: Session = Depends(get_db)):
    """שעת הזהב - כרטיסים לפי שעת הקרנה"""
    results = (
        db.query(
            func.strftime('%H', Screening.showtime).label("hour"),
            func.sum(Screening.tickets_sold).label("tickets"),
            func.count(Screening.id).label("screenings"),
            func.avg(Screening.tickets_sold * 100.0 / func.nullif(Screening.total_seats, 0)).label("avg_occupancy"),
        )
        .filter(Screening.showtime.isnot(None))
        .group_by("hour")
        .order_by("hour")
        .all()
    )

    return [
        {
            "hour": h,
            "hour_display": f"{h}:00",
            "tickets_sold": t or 0,
            "screenings_count": s,
            "avg_occupancy": round(o or 0, 1),
        }
        for h, t, s, o in results
    ]


@router.get("/analytics/occupancy-by-day-of-week")
def get_occupancy_by_day_of_week(db: Session = Depends(get_db)):
    """תפוסה לפי יום בשבוע"""
    day_names_he = {
        '0': 'ראשון', '1': 'שני', '2': 'שלישי', '3': 'רביעי',
        '4': 'חמישי', '5': 'שישי', '6': 'שבת',
    }
    results = (
        db.query(
            func.strftime('%w', Screening.showtime).label("dow"),
            func.sum(Screening.tickets_sold).label("tickets"),
            func.count(Screening.id).label("screenings"),
            func.avg(Screening.tickets_sold * 100.0 / func.nullif(Screening.total_seats, 0)).label("avg_occupancy"),
        )
        .filter(Screening.showtime.isnot(None))
        .group_by("dow")
        .order_by("dow")
        .all()
    )

    return [
        {
            "day_index": int(d),
            "day_name": day_names_he.get(d, d),
            "tickets_sold": t or 0,
            "screenings_count": s,
            "avg_occupancy": round(o or 0, 1),
        }
        for d, t, s, o in results
    ]


@router.get("/analytics/movie-trends")
def get_movie_trends(db: Session = Depends(get_db)):
    """מגמות סרטים - השוואה בין 3 ימים אחרונים ל-3 ימים לפני"""
    now = datetime.now()
    recent_start = now - timedelta(days=3)
    prev_start = now - timedelta(days=6)

    # Recent 3 days
    recent = dict(
        db.query(
            Movie.id,
            func.sum(Screening.tickets_sold),
        )
        .join(Screening)
        .filter(Screening.showtime >= recent_start)
        .group_by(Movie.id)
        .all()
    )

    # Previous 3 days
    previous = dict(
        db.query(
            Movie.id,
            func.sum(Screening.tickets_sold),
        )
        .join(Screening)
        .filter(Screening.showtime >= prev_start, Screening.showtime < recent_start)
        .group_by(Movie.id)
        .all()
    )

    movie_ids = set(recent.keys()) | set(previous.keys())
    movies = {m.id: m for m in db.query(Movie).filter(Movie.id.in_(movie_ids)).all()}

    trends = []
    for mid in movie_ids:
        movie = movies.get(mid)
        if not movie:
            continue
        r = recent.get(mid, 0) or 0
        p = previous.get(mid, 0) or 0
        change_pct = round(((r - p) / p * 100), 1) if p > 0 else (100.0 if r > 0 else 0)
        trends.append({
            "movie_id": mid,
            "title": movie.title,
            "title_he": movie.title_he,
            "recent_tickets": r,
            "previous_tickets": p,
            "change_pct": change_pct,
            "trend": "up" if r > p else ("down" if r < p else "stable"),
        })

    trends.sort(key=lambda x: x["recent_tickets"], reverse=True)
    return trends[:15]


@router.get("/analytics/dead-screenings")
def get_dead_screenings(threshold: float = Query(default=10.0), db: Session = Depends(get_db)):
    """הקרנות מתות - תפוסה מתחת לסף"""
    results = (
        db.query(
            Screening, Movie.title, Movie.title_he, Cinema.name, Cinema.city,
        )
        .join(Movie, Screening.movie_id == Movie.id)
        .join(Cinema, Screening.cinema_id == Cinema.id)
        .filter(
            Screening.total_seats > 0,
            (Screening.tickets_sold * 100.0 / Screening.total_seats) < threshold,
        )
        .order_by(Screening.showtime.desc())
        .limit(50)
        .all()
    )

    total_screenings = db.query(Screening).filter(Screening.total_seats > 0).count()
    dead_count = (
        db.query(Screening)
        .filter(
            Screening.total_seats > 0,
            (Screening.tickets_sold * 100.0 / Screening.total_seats) < threshold,
        )
        .count()
    )

    return {
        "threshold": threshold,
        "dead_count": dead_count,
        "total_screenings": total_screenings,
        "dead_pct": round(dead_count * 100 / total_screenings, 1) if total_screenings > 0 else 0,
        "screenings": [
            {
                "movie": th or t,
                "cinema": cn,
                "city": cc,
                "showtime": scr.showtime.isoformat() if scr.showtime else None,
                "hall": scr.hall or "",
                "format": scr.format or "",
                "tickets_sold": scr.tickets_sold or 0,
                "total_seats": scr.total_seats or 0,
                "occupancy": round((scr.tickets_sold or 0) * 100 / scr.total_seats, 1) if scr.total_seats else 0,
            }
            for scr, t, th, cn, cc in results
        ],
    }


@router.get("/analytics/format-by-branch")
def get_format_by_branch(db: Session = Depends(get_db)):
    """ניתוח פורמט לפי סניף"""
    results = (
        db.query(
            Cinema.name,
            Cinema.city,
            Screening.format,
            func.count(Screening.id).label("screenings"),
            func.sum(Screening.tickets_sold).label("tickets"),
            func.avg(Screening.tickets_sold * 100.0 / func.nullif(Screening.total_seats, 0)).label("avg_occupancy"),
        )
        .join(Cinema, Screening.cinema_id == Cinema.id)
        .group_by(Cinema.id, Screening.format)
        .order_by(desc("tickets"))
        .all()
    )

    return [
        {
            "cinema": n,
            "city": c,
            "format": f or "רגיל",
            "screenings_count": s,
            "tickets_sold": t or 0,
            "avg_occupancy": round(o or 0, 1),
        }
        for n, c, f, s, t, o in results
    ]


@router.get("/analytics/branch-efficiency")
def get_branch_efficiency(db: Session = Depends(get_db)):
    """דירוג סניפים לפי יעילות (ממוצע תפוסה)"""
    results = (
        db.query(
            Cinema.name,
            Cinema.city,
            CinemaChain.name.label("chain"),
            func.count(Screening.id).label("screenings"),
            func.sum(Screening.tickets_sold).label("total_tickets"),
            func.sum(Screening.total_seats).label("total_seats"),
            func.avg(Screening.tickets_sold * 100.0 / func.nullif(Screening.total_seats, 0)).label("avg_occupancy"),
        )
        .join(Cinema, Screening.cinema_id == Cinema.id)
        .join(CinemaChain, Cinema.chain_id == CinemaChain.id)
        .group_by(Cinema.id)
        .having(func.count(Screening.id) >= 5)
        .order_by(desc("avg_occupancy"))
        .all()
    )

    return [
        {
            "cinema": n,
            "city": c,
            "chain": ch,
            "screenings_count": s,
            "total_tickets": tt or 0,
            "total_seats": ts or 0,
            "avg_occupancy": round(o or 0, 1),
            "fill_rate": round((tt or 0) * 100 / ts, 1) if ts else 0,
        }
        for n, c, ch, s, tt, ts, o in results
    ]


@router.get("/analytics/genre-stats")
def get_genre_stats(db: Session = Depends(get_db)):
    """סטטיסטיקות לפי ז'אנר"""
    results = (
        db.query(
            Movie.genre,
            func.count(func.distinct(Movie.id)).label("movies_count"),
            func.count(Screening.id).label("screenings"),
            func.sum(Screening.tickets_sold).label("tickets"),
            func.avg(Screening.tickets_sold * 100.0 / func.nullif(Screening.total_seats, 0)).label("avg_occupancy"),
        )
        .join(Screening)
        .filter(Movie.genre.isnot(None), Movie.genre != '')
        .group_by(Movie.genre)
        .order_by(desc("tickets"))
        .all()
    )

    return [
        {
            "genre": g,
            "movies_count": mc,
            "screenings_count": s,
            "total_tickets": t or 0,
            "avg_occupancy": round(o or 0, 1),
        }
        for g, mc, s, t, o in results
    ]


# ─── Manual Scrape Trigger ────────────────────────────────────────────

@router.post("/scrape/trigger")
async def trigger_scrape(chain: Optional[str] = Query(default=None)):
    """הפעלת סריקה ידנית - רץ ברקע.

    chain: "hot_cinema", "movieland", or None (both concurrently)
    """
    from scrapers.manager import run_initial_scrape, run_movieland_initial_scrape

    async def run_hot():
        db = SessionLocal()
        try:
            await run_initial_scrape(db)
        except Exception as e:
            logger.error(f"Manual Hot Cinema scrape failed: {e}")
        finally:
            db.close()

    async def run_mvl():
        db = SessionLocal()
        try:
            await run_movieland_initial_scrape(db)
        except Exception as e:
            logger.error(f"Manual Movieland scrape failed: {e}")
        finally:
            db.close()

    if chain == "hot_cinema":
        asyncio.create_task(run_hot())
        return {"status": "started", "message": "סריקת הוט סינמה הופעלה ברקע."}
    elif chain == "movieland":
        asyncio.create_task(run_mvl())
        return {"status": "started", "message": "סריקת מובילנד הופעלה ברקע."}
    else:
        asyncio.create_task(run_hot())
        asyncio.create_task(run_mvl())
        return {"status": "started", "message": "סריקת שתי הרשתות הופעלה ברקע (במקביל)."}


@router.post("/scrape/tickets")
async def trigger_ticket_scan(chain: Optional[str] = Query(default=None)):
    """סריקת כיסאות ידנית - רץ ברקע.

    chain: "hot_cinema", "movieland", or None (both sequentially)
    """
    from scrapers.manager import hot_cinema_update_tickets, movieland_update_tickets

    async def run_hot_tickets():
        db = SessionLocal()
        try:
            await hot_cinema_update_tickets(db)
        except Exception as e:
            logger.error(f"Manual Hot Cinema ticket scan failed: {e}")
        finally:
            db.close()

    async def run_mvl_tickets():
        db = SessionLocal()
        try:
            await movieland_update_tickets(db)
        except Exception as e:
            logger.error(f"Manual Movieland ticket scan failed: {e}")
        finally:
            db.close()

    if chain == "hot_cinema":
        asyncio.create_task(run_hot_tickets())
        return {"status": "started", "message": "סריקת כיסאות הוט סינמה הופעלה ברקע."}
    elif chain == "movieland":
        asyncio.create_task(run_mvl_tickets())
        return {"status": "started", "message": "סריקת כיסאות מובילנד הופעלה ברקע."}
    else:
        asyncio.create_task(run_hot_tickets())
        asyncio.create_task(run_mvl_tickets())
        return {"status": "started", "message": "סריקת כיסאות שתי הרשתות הופעלה ברקע (במקביל)."}


# ─── Scrape Logs ─────────────────────────────────────────────────────

@router.get("/scrape-logs")
def get_scrape_logs(limit: int = Query(default=20), db: Session = Depends(get_db)):
    """לוגים של סריקות אחרונות"""
    logs = (
        db.query(ScrapeLog)
        .order_by(desc(ScrapeLog.created_at))
        .limit(limit)
        .all()
    )

    import json as _json
    return [
        {
            "id": log.id,
            "chain_name": log.chain_name,
            "status": log.status,
            "movies_found": log.movies_found,
            "screenings_found": log.screenings_found,
            "error_message": log.error_message,
            "duration_seconds": log.duration_seconds,
            "progress": _json.loads(log.progress) if log.progress else None,
            "created_at": log.created_at.isoformat() if log.created_at else None,
        }
        for log in logs
    ]


@router.get("/analytics/movie-lifetime/{movie_id}")
def get_movie_lifetime(movie_id: int, db: Session = Depends(get_db)):
    """סטטיסטיקות מלאות של סרט לאורך כל חייו בקולנוע"""
    movie = db.query(Movie).filter(Movie.id == movie_id).first()
    if not movie:
        return {"error": "Movie not found"}

    screenings = (
        db.query(Screening)
        .filter(Screening.movie_id == movie_id)
        .all()
    )

    total_tickets = sum(s.tickets_sold or 0 for s in screenings)
    total_seats = sum(s.total_seats or 0 for s in screenings)
    showtimes = [s.showtime for s in screenings if s.showtime]
    first_screening = min(showtimes).isoformat() if showtimes else None
    last_screening = max(showtimes).isoformat() if showtimes else None

    # פירוט לפי תאריך
    by_date = (
        db.query(
            func.date(Screening.showtime).label("date"),
            func.sum(Screening.tickets_sold).label("tickets"),
            func.count(Screening.id).label("screenings"),
        )
        .filter(Screening.movie_id == movie_id)
        .group_by(func.date(Screening.showtime))
        .order_by("date")
        .all()
    )

    return {
        "movie_id": movie.id,
        "title": movie.title,
        "title_he": movie.title_he,
        "total_tickets_sold": total_tickets,
        "total_seats": total_seats,
        "total_screenings": len(screenings),
        "avg_occupancy": round(total_tickets * 100 / total_seats, 1) if total_seats > 0 else 0,
        "first_screening": first_screening,
        "last_screening": last_screening,
        "by_date": [
            {"date": str(d), "tickets_sold": t or 0, "screenings_count": s}
            for d, t, s in by_date
        ],
    }


@router.get("/analytics/blocked-seats")
def get_blocked_seats_stats(db: Session = Depends(get_db)):
    """מעקב אחר זיהוי מושבים חסומים - מציג את מצב הלמידה"""
    import json as _json2

    stats = (
        db.query(HallSeatStats, Cinema.name, Cinema.city)
        .join(Cinema, HallSeatStats.cinema_id == Cinema.id)
        .order_by(HallSeatStats.blocked_count.desc(), HallSeatStats.scan_count.desc())
        .all()
    )

    total_blocked = sum(s.blocked_count for s, _, _ in stats)
    total_scans = sum(s.scan_count for s, _, _ in stats)

    return {
        "summary": {
            "halls_tracked": len(stats),
            "total_scans": total_scans,
            "total_blocked_seats": total_blocked,
        },
        "halls": [
            {
                "cinema": name,
                "city": city,
                "hall": s.hall,
                "scan_count": s.scan_count,
                "blocked_count": s.blocked_count,
                "tracked_seats": len(_json2.loads(s.seat_sold_counts)) if s.seat_sold_counts else 0,
                "blocked_seats": _json2.loads(s.blocked_seats) if s.blocked_seats else [],
                "updated_at": s.updated_at.isoformat() if s.updated_at else None,
            }
            for s, name, city in stats
        ],
    }
