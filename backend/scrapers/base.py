import httpx
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class ScrapedMovie:
    def __init__(self, title: str, title_he: str = "", genre: str = "",
                 duration_minutes: int = 0, release_date: str = "",
                 poster_url: str = "", rating: str = "", director: str = "",
                 detail_url: str = ""):
        self.title = title
        self.title_he = title_he
        self.genre = genre
        self.duration_minutes = duration_minutes
        self.release_date = release_date
        self.poster_url = poster_url
        self.rating = rating
        self.director = director
        self.detail_url = detail_url


class ScrapedScreening:
    def __init__(self, movie_title: str, cinema_name: str, city: str,
                 showtime: datetime, hall: str = "", format: str = "2D",
                 language: str = "", ticket_price: float = 0,
                 tickets_sold: int = 0, total_seats: int = 0,
                 revenue: float = 0):
        self.movie_title = movie_title
        self.cinema_name = cinema_name
        self.city = city
        self.showtime = showtime
        self.hall = hall
        self.format = format
        self.language = language
        self.ticket_price = ticket_price
        self.tickets_sold = tickets_sold
        self.total_seats = total_seats
        self.revenue = revenue


class BaseScraper(ABC):
    """Base class for cinema chain scrapers"""

    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=30.0,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
            },
            follow_redirects=True,
        )

    @property
    @abstractmethod
    def chain_name(self) -> str:
        pass

    @property
    @abstractmethod
    def chain_name_he(self) -> str:
        pass

    @property
    @abstractmethod
    def base_url(self) -> str:
        pass

    @abstractmethod
    async def scrape_movies(self) -> list[ScrapedMovie]:
        pass

    @abstractmethod
    async def scrape_screenings(self) -> list[ScrapedScreening]:
        pass

    async def run(self) -> tuple[list[ScrapedMovie], list[ScrapedScreening]]:
        start = datetime.utcnow()
        try:
            movies = await self.scrape_movies()
            screenings = await self.scrape_screenings()
            duration = (datetime.utcnow() - start).total_seconds()
            logger.info(
                f"[{self.chain_name}] Scraped {len(movies)} movies, "
                f"{len(screenings)} screenings in {duration:.1f}s"
            )
            return movies, screenings
        except Exception as e:
            logger.error(f"[{self.chain_name}] Scrape failed: {e}")
            raise

    async def close(self):
        await self.client.aclose()
