"""
Scraper manager - coordinates all scrapers and persists data to DB.
"""
import logging
from datetime import datetime
from sqlalchemy.orm import Session

from models.models import CinemaChain, Cinema, Movie, Screening, ScrapeLog
from scrapers.base import BaseScraper, ScrapedMovie, ScrapedScreening
from scrapers.cinema_city import CinemaCityScraper
from scrapers.hot_cinema import HotCinemaScraper
from scrapers.lev_cinema import LevCinemaScraper
from scrapers.globus_max import GlobusMaxScraper

logger = logging.getLogger(__name__)

ALL_SCRAPERS: list[type[BaseScraper]] = [
    CinemaCityScraper,
    HotCinemaScraper,
    LevCinemaScraper,
    GlobusMaxScraper,
]


def _get_or_create_chain(db: Session, name: str, name_he: str, website: str) -> CinemaChain:
    chain = db.query(CinemaChain).filter_by(name=name).first()
    if not chain:
        chain = CinemaChain(name=name, name_he=name_he, website=website)
        db.add(chain)
        db.flush()
    return chain


def _get_or_create_cinema(db: Session, chain_id: int, name: str, city: str) -> Cinema:
    cinema = db.query(Cinema).filter_by(name=name, chain_id=chain_id).first()
    if not cinema:
        cinema = Cinema(chain_id=chain_id, name=name, city=city)
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


async def run_all_scrapers(db: Session):
    """Run all scrapers and persist results to the database."""
    for scraper_cls in ALL_SCRAPERS:
        scraper = scraper_cls()
        start = datetime.utcnow()
        try:
            movies, screenings = await scraper.run()

            chain = _get_or_create_chain(
                db, scraper.chain_name, scraper.chain_name_he, scraper.base_url
            )

            for sm in movies:
                _get_or_create_movie(db, sm)

            for ss in screenings:
                cinema = _get_or_create_cinema(db, chain.id, ss.cinema_name, ss.city)
                movie = db.query(Movie).filter_by(title=ss.movie_title).first()
                if not movie:
                    movie = _get_or_create_movie(db, ScrapedMovie(title=ss.movie_title))

                existing = db.query(Screening).filter_by(
                    movie_id=movie.id,
                    cinema_id=cinema.id,
                    showtime=ss.showtime,
                ).first()

                if existing:
                    existing.tickets_sold = ss.tickets_sold
                    existing.revenue = ss.revenue
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
                        revenue=ss.revenue,
                    )
                    db.add(screening)

            duration = (datetime.utcnow() - start).total_seconds()
            log = ScrapeLog(
                chain_name=scraper.chain_name,
                status="success",
                movies_found=len(movies),
                screenings_found=len(screenings),
                duration_seconds=duration,
            )
            db.add(log)
            db.commit()

        except Exception as e:
            duration = (datetime.utcnow() - start).total_seconds()
            log = ScrapeLog(
                chain_name=scraper.chain_name,
                status="error",
                error_message=str(e),
                duration_seconds=duration,
            )
            db.add(log)
            db.commit()
            logger.error(f"Scraper {scraper.chain_name} failed: {e}")

        finally:
            await scraper.close()
