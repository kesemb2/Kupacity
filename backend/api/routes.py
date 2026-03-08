from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, desc
from typing import Optional
from datetime import datetime, timedelta

from database import get_db
from models.models import CinemaChain, Cinema, Movie, Screening, ScrapeLog

router = APIRouter(prefix="/api")


# ─── Dashboard Summary ───────────────────────────────────────────────

@router.get("/dashboard/summary")
def get_dashboard_summary(db: Session = Depends(get_db)):
    """סיכום כללי לדאשבורד הראשי"""
    total_movies = db.query(Movie).count()
    total_cinemas = db.query(Cinema).count()
    total_chains = db.query(CinemaChain).count()
    total_screenings = db.query(Screening).count()
    total_revenue = db.query(func.sum(Screening.revenue)).scalar() or 0
    total_tickets = db.query(func.sum(Screening.tickets_sold)).scalar() or 0

    # Top movie by revenue
    top_movie_row = (
        db.query(
            Movie.title, Movie.title_he,
            func.sum(Screening.revenue).label("total_revenue"),
            func.sum(Screening.tickets_sold).label("total_tickets"),
        )
        .join(Screening)
        .group_by(Movie.id)
        .order_by(desc("total_revenue"))
        .first()
    )

    return {
        "total_movies": total_movies,
        "total_cinemas": total_cinemas,
        "total_chains": total_chains,
        "total_screenings": total_screenings,
        "total_revenue": round(total_revenue, 2),
        "total_tickets_sold": total_tickets,
        "top_movie": {
            "title": top_movie_row[0] if top_movie_row else None,
            "title_he": top_movie_row[1] if top_movie_row else None,
            "revenue": round(top_movie_row[2], 2) if top_movie_row else 0,
            "tickets_sold": top_movie_row[3] if top_movie_row else 0,
        } if top_movie_row else None,
    }


# ─── Movies ──────────────────────────────────────────────────────────

@router.get("/movies")
def get_movies(db: Session = Depends(get_db)):
    """רשימת כל הסרטים עם סיכום הכנסות"""
    results = (
        db.query(
            Movie,
            func.count(Screening.id).label("screenings_count"),
            func.sum(Screening.tickets_sold).label("total_tickets"),
            func.sum(Screening.revenue).label("total_revenue"),
            func.avg(Screening.tickets_sold * 100.0 / func.nullif(Screening.total_seats, 0)).label("avg_occupancy"),
        )
        .outerjoin(Screening)
        .group_by(Movie.id)
        .order_by(desc("total_revenue"))
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
            "total_revenue": round(total_revenue or 0, 2),
            "avg_occupancy": round(avg_occ or 0, 1),
        }
        for movie, screenings_count, total_tickets, total_revenue, avg_occ in results
    ]


@router.get("/movies/{movie_id}")
def get_movie_detail(movie_id: int, db: Session = Depends(get_db)):
    """פרטי סרט עם פירוט לפי בתי קולנוע"""
    movie = db.query(Movie).filter_by(id=movie_id).first()
    if not movie:
        return {"error": "Movie not found"}

    # Revenue by cinema
    by_cinema = (
        db.query(
            Cinema.name, Cinema.city,
            CinemaChain.name.label("chain_name"),
            func.count(Screening.id).label("screenings"),
            func.sum(Screening.tickets_sold).label("tickets"),
            func.sum(Screening.revenue).label("revenue"),
        )
        .join(Screening, Screening.cinema_id == Cinema.id)
        .join(CinemaChain, Cinema.chain_id == CinemaChain.id)
        .filter(Screening.movie_id == movie_id)
        .group_by(Cinema.id)
        .order_by(desc("revenue"))
        .all()
    )

    # Revenue by date
    by_date = (
        db.query(
            func.date(Screening.showtime).label("date"),
            func.sum(Screening.tickets_sold).label("tickets"),
            func.sum(Screening.revenue).label("revenue"),
        )
        .filter(Screening.movie_id == movie_id)
        .group_by(func.date(Screening.showtime))
        .order_by("date")
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
                "chain": chain_name,
                "screenings": s,
                "tickets_sold": t,
                "revenue": round(r, 2),
            }
            for name, city, chain_name, s, t, r in by_cinema
        ],
        "by_date": [
            {"date": str(d), "tickets_sold": t, "revenue": round(r, 2)}
            for d, t, r in by_date
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
            func.sum(Screening.revenue).label("total_revenue"),
        )
        .join(CinemaChain, Cinema.chain_id == CinemaChain.id)
        .outerjoin(Screening)
        .group_by(Cinema.id)
        .order_by(desc("total_revenue"))
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
            "total_revenue": round(tr or 0, 2),
        }
        for cinema, chain_name, chain_he, sc, tt, tr in results
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
            func.sum(Screening.revenue).label("total_revenue"),
        )
        .outerjoin(Screening)
        .group_by(Cinema.city)
        .order_by(desc("total_revenue"))
        .all()
    )

    return [
        {
            "city": city,
            "city_he": city_he,
            "cinemas_count": cc,
            "screenings_count": sc or 0,
            "total_tickets_sold": tt or 0,
            "total_revenue": round(tr or 0, 2),
        }
        for city, city_he, cc, sc, tt, tr in results
    ]


# ─── Chains ──────────────────────────────────────────────────────────

@router.get("/chains")
def get_chains(db: Session = Depends(get_db)):
    """סיכום לפי רשתות קולנוע"""
    results = (
        db.query(
            CinemaChain,
            func.count(func.distinct(Cinema.id)).label("cinemas_count"),
            func.count(Screening.id).label("screenings_count"),
            func.sum(Screening.tickets_sold).label("total_tickets"),
            func.sum(Screening.revenue).label("total_revenue"),
        )
        .outerjoin(Cinema)
        .outerjoin(Screening)
        .group_by(CinemaChain.id)
        .order_by(desc("total_revenue"))
        .all()
    )

    return [
        {
            "id": chain.id,
            "name": chain.name,
            "name_he": chain.name_he,
            "website": chain.website,
            "cinemas_count": cc,
            "screenings_count": sc or 0,
            "total_tickets_sold": tt or 0,
            "total_revenue": round(tr or 0, 2),
        }
        for chain, cc, sc, tt, tr in results
    ]


# ─── Revenue Over Time ──────────────────────────────────────────────

@router.get("/analytics/revenue-by-date")
def get_revenue_by_date(days: int = Query(default=14), db: Session = Depends(get_db)):
    """הכנסות לפי תאריך"""
    cutoff = datetime.now() - timedelta(days=days)
    results = (
        db.query(
            func.date(Screening.showtime).label("date"),
            func.sum(Screening.revenue).label("revenue"),
            func.sum(Screening.tickets_sold).label("tickets"),
            func.count(Screening.id).label("screenings"),
        )
        .filter(Screening.showtime >= cutoff)
        .group_by(func.date(Screening.showtime))
        .order_by("date")
        .all()
    )

    return [
        {
            "date": str(d),
            "revenue": round(r, 2),
            "tickets_sold": t,
            "screenings_count": s,
        }
        for d, r, t, s in results
    ]


@router.get("/analytics/revenue-by-chain")
def get_revenue_by_chain(db: Session = Depends(get_db)):
    """הכנסות לפי רשת קולנוע"""
    results = (
        db.query(
            CinemaChain.name,
            CinemaChain.name_he,
            func.sum(Screening.revenue).label("revenue"),
            func.sum(Screening.tickets_sold).label("tickets"),
        )
        .join(Cinema, Cinema.chain_id == CinemaChain.id)
        .join(Screening, Screening.cinema_id == Cinema.id)
        .group_by(CinemaChain.id)
        .order_by(desc("revenue"))
        .all()
    )

    return [
        {"name": n, "name_he": nh, "revenue": round(r, 2), "tickets_sold": t}
        for n, nh, r, t in results
    ]


@router.get("/analytics/top-movies")
def get_top_movies(limit: int = Query(default=10), db: Session = Depends(get_db)):
    """הסרטים המובילים"""
    results = (
        db.query(
            Movie.title,
            Movie.title_he,
            Movie.genre,
            func.sum(Screening.revenue).label("total_revenue"),
            func.sum(Screening.tickets_sold).label("total_tickets"),
            func.count(Screening.id).label("screenings"),
        )
        .join(Screening)
        .group_by(Movie.id)
        .order_by(desc("total_revenue"))
        .limit(limit)
        .all()
    )

    return [
        {
            "title": t, "title_he": th, "genre": g,
            "total_revenue": round(r, 2), "total_tickets_sold": tt,
            "screenings_count": s,
        }
        for t, th, g, r, tt, s in results
    ]


@router.get("/analytics/occupancy-by-format")
def get_occupancy_by_format(db: Session = Depends(get_db)):
    """תפוסה לפי פורמט הקרנה"""
    results = (
        db.query(
            Screening.format,
            func.count(Screening.id).label("screenings"),
            func.avg(Screening.tickets_sold * 100.0 / func.nullif(Screening.total_seats, 0)).label("avg_occupancy"),
            func.sum(Screening.revenue).label("total_revenue"),
        )
        .group_by(Screening.format)
        .order_by(desc("total_revenue"))
        .all()
    )

    return [
        {
            "format": f,
            "screenings_count": s,
            "avg_occupancy": round(o or 0, 1),
            "total_revenue": round(r or 0, 2),
        }
        for f, s, o, r in results
    ]


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

    return [
        {
            "id": log.id,
            "chain_name": log.chain_name,
            "status": log.status,
            "movies_found": log.movies_found,
            "screenings_found": log.screenings_found,
            "error_message": log.error_message,
            "duration_seconds": log.duration_seconds,
            "created_at": log.created_at.isoformat() if log.created_at else None,
        }
        for log in logs
    ]
