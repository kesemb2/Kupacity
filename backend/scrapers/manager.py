"""
Scraper manager - coordinates scrapers and persists data to DB.

Hot Cinema schedule:
- Weekly:       full movie catalog refresh
- Daily:        screening schedule refresh (next 7 days)
- Every 5h:     ticket count updates for active screenings
- Every 1 min:  close screenings that started 10+ minutes ago

Movieland schedule:
- Weekly:       full movie catalog refresh
- Daily:        screening schedule refresh
- Every 5h:     ticket count updates (offset from Hot Cinema)
"""
import json
import logging
from datetime import datetime, timedelta
from sqlalchemy.orm import Session

from models.models import CinemaChain, Cinema, Movie, Screening, ScrapeLog, HallSeatStats
from scrapers.base import BaseScraper, ScrapedMovie, ScrapedScreening
from scrapers.hot_cinema import HotCinemaScraper, HOT_CINEMA_BRANCHES
from scrapers.movieland import MovielandScraper, MOVIELAND_BRANCHES

logger = logging.getLogger(__name__)


def _get_or_create_chain(db: Session, name: str, name_he: str, website: str) -> CinemaChain:
    chain = db.query(CinemaChain).filter_by(name=name).first()
    if not chain:
        chain = CinemaChain(name=name, name_he=name_he, website=website)
        db.add(chain)
        db.flush()
    return chain


def _get_or_create_cinema(db: Session, chain_id: int, name: str, city: str,
                          name_he: str = "", city_he: str = "") -> Cinema:
    cinema = db.query(Cinema).filter_by(name=name, chain_id=chain_id).first()
    if not cinema:
        cinema = Cinema(chain_id=chain_id, name=name, city=city,
                        name_he=name_he, city_he=city_he)
        db.add(cinema)
        db.flush()
    return cinema


def _get_or_create_movie(db: Session, scraped: ScrapedMovie) -> Movie:
    movie = db.query(Movie).filter_by(title=scraped.title).first()
    if not movie:
        movie = Movie(
            title=scraped.title,
            title_he=scraped.title_he,
            genre=scraped.genre,
            duration_minutes=scraped.duration_minutes,
            release_date=scraped.release_date,
            poster_url=scraped.poster_url,
            rating=scraped.rating,
            director=scraped.director,
        )
        db.add(movie)
        db.flush()
    return movie


def _lookup_hebrew(cinema_name: str) -> tuple[str, str]:
    """Look up Hebrew name and city for a cinema branch (any chain)."""
    for branch in HOT_CINEMA_BRANCHES.values():
        if branch["name"] == cinema_name:
            return branch["name_he"], branch["city_he"]
    for branch in MOVIELAND_BRANCHES.values():
        if branch["name"] == cinema_name:
            return branch["name_he"], branch["city_he"]
    return "", ""


def _finalize_blocked_seats(db: Session, hall_data: dict) -> None:
    """After a scrape run, update blocked-seat stats once per hall.

    hall_data: {(cinema_id, hall): [set_of_pos_keys, ...]}
        Each set contains "x,y" keys of sold seats from one screening.
        A seat is "always-sold in this run" only if it appears in ALL screenings
        of that hall (intersection).  scan_count increments once per run.
    """
    for (cinema_id, hall), position_sets in hall_data.items():
        if not position_sets:
            continue

        # Intersection: seats sold in ALL screenings of this hall in this run
        always_sold = position_sets[0]
        for ps in position_sets[1:]:
            always_sold = always_sold & ps
        if not always_sold:
            continue

        stats = db.query(HallSeatStats).filter_by(cinema_id=cinema_id, hall=hall).first()
        if not stats:
            stats = HallSeatStats(cinema_id=cinema_id, hall=hall)
            db.add(stats)
            db.flush()

        stats.scan_count += 1
        sold_counts = json.loads(stats.seat_sold_counts)

        for key in always_sold:
            sold_counts[key] = sold_counts.get(key, 0) + 1

        stats.seat_sold_counts = json.dumps(sold_counts)

        # Recompute blocked seats: >= 90% of scan runs, minimum 3 runs
        blocked = []
        if stats.scan_count >= 3:
            threshold = 0.90
            for key, count in sold_counts.items():
                if count / stats.scan_count >= threshold:
                    blocked.append(key)

        stats.blocked_seats = json.dumps(blocked)
        stats.blocked_count = len(blocked)
        stats.updated_at = datetime.utcnow()

        # Update all active screenings in this hall with the new blocked count
        db.query(Screening).filter(
            Screening.cinema_id == cinema_id,
            Screening.hall == hall,
            Screening.status == "active",
        ).update({"blocked_seats_excluded": stats.blocked_count}, synchronize_session="fetch")

    try:
        db.commit()
    except Exception:
        db.rollback()


def _upsert_screenings(db: Session, chain: CinemaChain, screenings: list[ScrapedScreening]):
    """Insert new screenings or update existing ones (tickets_sold)."""
    for ss in screenings:
        name_he, city_he = _lookup_hebrew(ss.cinema_name)
        cinema = _get_or_create_cinema(db, chain.id, ss.cinema_name, ss.city,
                                       name_he=name_he, city_he=city_he)
        movie = db.query(Movie).filter_by(title=ss.movie_title).first()
        if not movie:
            movie = _get_or_create_movie(db, ScrapedMovie(title=ss.movie_title))

        existing = db.query(Screening).filter_by(
            movie_id=movie.id,
            cinema_id=cinema.id,
            showtime=ss.showtime,
        ).first()

        if existing:
            # Only update seat data if the new scrape has real data
            # (total_seats > 0 means seat map was successfully read)
            if ss.total_seats > 0:
                existing.tickets_sold = ss.tickets_sold
                existing.total_seats = ss.total_seats
            existing.scraped_at = datetime.utcnow()
        else:
            screening = Screening(
                movie_id=movie.id,
                cinema_id=cinema.id,
                showtime=ss.showtime,
                hall=ss.hall,
                format=ss.format,
                language=ss.language,
                ticket_price=ss.ticket_price,
                tickets_sold=ss.tickets_sold,
                total_seats=ss.total_seats,
                status="active",
            )
            db.add(screening)



# ---------------------------------------------------------------------------
# Hot Cinema specific scheduled tasks
# ---------------------------------------------------------------------------

def _make_progress_callback(db: Session, log: ScrapeLog):
    """Create a callback that updates a ScrapeLog's progress field in real-time."""
    def on_progress(phase: str, current: int, total: int, detail: str = ""):
        try:
            log.progress = json.dumps(
                {"phase": phase, "current": current, "total": total, "detail": detail},
                ensure_ascii=False,
            )
            db.commit()
        except Exception:
            db.rollback()
    return on_progress


def _make_screening_callback(db: Session, chain: CinemaChain, log: ScrapeLog):
    """Create a callback that saves each screening to DB immediately.

    Returns (callback_fn, hall_data) — hall_data accumulates sold positions
    per (cinema_id, hall) for blocked-seat learning at end of run.
    """
    saved_count = [0]  # mutable counter
    hall_data = {}  # {(cinema_id, hall): [set_of_pos_keys, ...]}

    def on_screening_update(screening: ScrapedScreening):
        try:
            _upsert_screenings(db, chain, [screening])
            db.commit()
            saved_count[0] += 1
            log.screenings_found = saved_count[0]
            db.commit()

            # Accumulate sold positions for blocked-seat learning
            if screening.sold_positions and screening.total_seats > 0 and screening.hall:
                name_he, city_he = _lookup_hebrew(screening.cinema_name)
                cinema = _get_or_create_cinema(db, chain.id, screening.cinema_name,
                                               screening.city, name_he=name_he, city_he=city_he)
                key = (cinema.id, screening.hall)
                pos_set = {f"{p[0]},{p[1]}" for p in screening.sold_positions}
                hall_data.setdefault(key, []).append(pos_set)
        except Exception:
            db.rollback()
    return on_screening_update, hall_data


async def run_initial_scrape(db: Session):
    """Run on startup if DB is empty - scrape movies and screenings from Hot Cinema."""
    scraper = HotCinemaScraper()
    start = datetime.utcnow()

    # Create a "running" log immediately so the UI can show progress
    log = ScrapeLog(chain_name="Hot Cinema", status="running",
                    progress=json.dumps({"phase": "מתחיל סריקה", "current": 0, "total": 0, "detail": ""}, ensure_ascii=False))
    db.add(log)
    db.commit()

    progress_cb = _make_progress_callback(db, log)

    try:
        movies = await scraper.scrape_movies(on_progress=progress_cb)
        chain = _get_or_create_chain(
            db, scraper.chain_name, scraper.chain_name_he, scraper.base_url
        )
        for sm in movies:
            _get_or_create_movie(db, sm)

        screenings = await scraper.scrape_screenings(on_progress=progress_cb)
        _upsert_screenings(db, chain, screenings)

        # Run ticket updates to get real seat counts (saved incrementally)
        screening_cb, hall_data = _make_screening_callback(db, chain, log)
        ticket_screenings = await scraper.scrape_ticket_updates(
            on_progress=progress_cb, on_screening_update=screening_cb,
        )
        _finalize_blocked_seats(db, hall_data)

        duration = (datetime.utcnow() - start).total_seconds()
        log.status = "success"
        log.movies_found = len(movies)
        log.screenings_found = len(screenings)
        log.duration_seconds = duration
        log.progress = None
        db.commit()
        logger.info(f"[Hot Cinema] Initial scrape: {len(movies)} movies, {len(screenings)} screenings")
    except Exception as e:
        duration = (datetime.utcnow() - start).total_seconds()
        log.status = "error"
        log.error_message = str(e)
        log.duration_seconds = duration
        log.progress = None
        db.commit()
        logger.error(f"[Hot Cinema] Initial scrape failed: {e}")
    finally:
        await scraper.close()


async def hot_cinema_weekly_movies(db: Session):
    """Weekly: refresh full movie catalog from Hot Cinema."""
    scraper = HotCinemaScraper()
    start = datetime.utcnow()

    log = ScrapeLog(chain_name="Hot Cinema", status="running",
                    progress=json.dumps({"phase": "סריקת סרטים שבועית", "current": 0, "total": 0, "detail": ""}, ensure_ascii=False))
    db.add(log)
    db.commit()
    progress_cb = _make_progress_callback(db, log)

    try:
        movies = await scraper.scrape_movies(on_progress=progress_cb)
        chain = _get_or_create_chain(
            db, scraper.chain_name, scraper.chain_name_he, scraper.base_url
        )
        for sm in movies:
            _get_or_create_movie(db, sm)

        duration = (datetime.utcnow() - start).total_seconds()
        log.status = "success"
        log.movies_found = len(movies)
        log.screenings_found = 0
        log.duration_seconds = duration
        log.progress = None
        db.commit()
        logger.info(f"[Hot Cinema] Weekly movies refresh: {len(movies)} movies")
    except Exception as e:
        duration = (datetime.utcnow() - start).total_seconds()
        log.status = "error"
        log.error_message = str(e)
        log.duration_seconds = duration
        log.progress = None
        db.commit()
        logger.error(f"[Hot Cinema] Weekly movies refresh failed: {e}")
    finally:
        await scraper.close()


async def hot_cinema_daily_screenings(db: Session):
    """Daily: refresh screening schedule for next 7 days."""
    scraper = HotCinemaScraper()
    start = datetime.utcnow()

    log = ScrapeLog(chain_name="Hot Cinema", status="running",
                    progress=json.dumps({"phase": "סריקת הקרנות יומית", "current": 0, "total": 0, "detail": ""}, ensure_ascii=False))
    db.add(log)
    db.commit()
    progress_cb = _make_progress_callback(db, log)

    try:
        screenings = await scraper.scrape_screenings(on_progress=progress_cb)
        chain = _get_or_create_chain(
            db, scraper.chain_name, scraper.chain_name_he, scraper.base_url
        )
        _upsert_screenings(db, chain, screenings)

        duration = (datetime.utcnow() - start).total_seconds()
        log.status = "success"
        log.movies_found = 0
        log.screenings_found = len(screenings)
        log.duration_seconds = duration
        log.progress = None
        db.commit()
        logger.info(f"[Hot Cinema] Daily screenings refresh: {len(screenings)} screenings")
    except Exception as e:
        duration = (datetime.utcnow() - start).total_seconds()
        log.status = "error"
        log.error_message = str(e)
        log.duration_seconds = duration
        log.progress = None
        db.commit()
        logger.error(f"[Hot Cinema] Daily screenings refresh failed: {e}")
    finally:
        await scraper.close()


async def hot_cinema_update_tickets(db: Session):
    """Every 5 hours: update ticket counts for today's active screenings."""
    scraper = HotCinemaScraper()
    start = datetime.utcnow()

    log = ScrapeLog(chain_name="Hot Cinema", status="running",
                    progress=json.dumps({"phase": "עדכון כרטיסים", "current": 0, "total": 0, "detail": ""}, ensure_ascii=False))
    db.add(log)
    db.commit()
    progress_cb = _make_progress_callback(db, log)

    try:
        chain = _get_or_create_chain(
            db, scraper.chain_name, scraper.chain_name_he, scraper.base_url
        )
        screening_cb, hall_data = _make_screening_callback(db, chain, log)
        screenings = await scraper.scrape_ticket_updates(
            on_progress=progress_cb, on_screening_update=screening_cb,
        )
        _finalize_blocked_seats(db, hall_data)

        duration = (datetime.utcnow() - start).total_seconds()
        log.status = "success"
        log.movies_found = 0
        log.screenings_found = len(screenings)
        log.duration_seconds = duration
        log.progress = None
        db.commit()
        logger.info(f"[Hot Cinema] Ticket update: {len(screenings)} screenings refreshed")
    except Exception as e:
        duration = (datetime.utcnow() - start).total_seconds()
        log.status = "error"
        log.error_message = str(e)
        log.duration_seconds = duration
        log.progress = None
        db.commit()
        logger.error(f"[Hot Cinema] Ticket update failed: {e}")
    finally:
        await scraper.close()


# ---------------------------------------------------------------------------
# Movieland specific scheduled tasks
# ---------------------------------------------------------------------------

async def movieland_weekly_movies(db: Session):
    """Weekly: refresh full movie catalog from Movieland."""
    scraper = MovielandScraper()
    start = datetime.utcnow()

    log = ScrapeLog(chain_name="Movieland", status="running",
                    progress=json.dumps({"phase": "סריקת סרטים שבועית - מובילנד", "current": 0, "total": 0, "detail": ""}, ensure_ascii=False))
    db.add(log)
    db.commit()
    progress_cb = _make_progress_callback(db, log)

    try:
        movies = await scraper.scrape_movies(on_progress=progress_cb)
        chain = _get_or_create_chain(
            db, scraper.chain_name, scraper.chain_name_he, scraper.base_url
        )
        for sm in movies:
            _get_or_create_movie(db, sm)

        duration = (datetime.utcnow() - start).total_seconds()
        log.status = "success"
        log.movies_found = len(movies)
        log.screenings_found = 0
        log.duration_seconds = duration
        log.progress = None
        db.commit()
        logger.info(f"[Movieland] Weekly movies refresh: {len(movies)} movies")
    except Exception as e:
        duration = (datetime.utcnow() - start).total_seconds()
        log.status = "error"
        log.error_message = str(e)
        log.duration_seconds = duration
        log.progress = None
        db.commit()
        logger.error(f"[Movieland] Weekly movies refresh failed: {e}")
    finally:
        await scraper.close()


async def movieland_daily_screenings(db: Session):
    """Daily: refresh screening schedule from Movieland."""
    scraper = MovielandScraper()
    start = datetime.utcnow()

    log = ScrapeLog(chain_name="Movieland", status="running",
                    progress=json.dumps({"phase": "סריקת הקרנות יומית - מובילנד", "current": 0, "total": 0, "detail": ""}, ensure_ascii=False))
    db.add(log)
    db.commit()
    progress_cb = _make_progress_callback(db, log)

    try:
        screenings = await scraper.scrape_screenings(on_progress=progress_cb)
        chain = _get_or_create_chain(
            db, scraper.chain_name, scraper.chain_name_he, scraper.base_url
        )
        _upsert_screenings(db, chain, screenings)

        duration = (datetime.utcnow() - start).total_seconds()
        log.status = "success"
        log.movies_found = 0
        log.screenings_found = len(screenings)
        log.duration_seconds = duration
        log.progress = None
        db.commit()
        logger.info(f"[Movieland] Daily screenings refresh: {len(screenings)} screenings")
    except Exception as e:
        duration = (datetime.utcnow() - start).total_seconds()
        log.status = "error"
        log.error_message = str(e)
        log.duration_seconds = duration
        log.progress = None
        db.commit()
        logger.error(f"[Movieland] Daily screenings refresh failed: {e}")
    finally:
        await scraper.close()


async def movieland_update_tickets(db: Session):
    """Every 5 hours: update ticket counts for Movieland screenings."""
    scraper = MovielandScraper()
    start = datetime.utcnow()

    log = ScrapeLog(chain_name="Movieland", status="running",
                    progress=json.dumps({"phase": "עדכון כרטיסים - מובילנד", "current": 0, "total": 0, "detail": ""}, ensure_ascii=False))
    db.add(log)
    db.commit()
    progress_cb = _make_progress_callback(db, log)

    try:
        chain = _get_or_create_chain(
            db, scraper.chain_name, scraper.chain_name_he, scraper.base_url
        )
        screening_cb, hall_data = _make_screening_callback(db, chain, log)
        screenings = await scraper.scrape_ticket_updates(
            on_progress=progress_cb, on_screening_update=screening_cb,
        )
        _finalize_blocked_seats(db, hall_data)

        duration = (datetime.utcnow() - start).total_seconds()
        log.status = "success"
        log.movies_found = 0
        log.screenings_found = len(screenings)
        log.duration_seconds = duration
        log.progress = None
        db.commit()
        logger.info(f"[Movieland] Ticket update: {len(screenings)} screenings refreshed")
    except Exception as e:
        duration = (datetime.utcnow() - start).total_seconds()
        log.status = "error"
        log.error_message = str(e)
        log.duration_seconds = duration
        log.progress = None
        db.commit()
        logger.error(f"[Movieland] Ticket update failed: {e}")
    finally:
        await scraper.close()


def close_expired_screenings(db: Session):
    """Every minute: close screenings that started 10+ minutes ago."""
    cutoff = datetime.utcnow() - timedelta(minutes=10)
    try:
        updated = (
            db.query(Screening)
            .filter(
                Screening.status == "active",
                Screening.showtime <= cutoff,
            )
            .update({"status": "closed"}, synchronize_session="fetch")
        )
        if updated:
            db.commit()
            logger.info(f"Closed {updated} expired screenings")
    except Exception:
        db.rollback()
        logger.debug("close_expired_screenings skipped - database busy")
