"""
Scraper for Lev Cinema (Lev Smadar, Lev Dizengoff, etc.)
Independent art-house cinema chain.
"""
import logging
from datetime import datetime
from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, ScrapedMovie, ScrapedScreening

logger = logging.getLogger(__name__)

LEV_BRANCHES = {
    "smadar": {"name": "Lev Smadar", "name_he": "לב סמדר", "city": "Jerusalem", "city_he": "ירושלים", "url": "https://www.lfrj.co.il"},
    "dizengoff": {"name": "Lev Dizengoff", "name_he": "לב דיזנגוף", "city": "Tel Aviv", "city_he": "תל אביב", "url": "https://www.lfrj.co.il"},
    "givatayim": {"name": "Lev Givatayim", "name_he": "לב גבעתיים", "city": "Givatayim", "city_he": "גבעתיים", "url": "https://www.lfrj.co.il"},
}


class LevCinemaScraper(BaseScraper):

    @property
    def chain_name(self) -> str:
        return "Lev Cinema"

    @property
    def chain_name_he(self) -> str:
        return "לב קולנוע"

    @property
    def base_url(self) -> str:
        return "https://www.lfrj.co.il"

    async def scrape_movies(self) -> list[ScrapedMovie]:
        movies = []
        try:
            resp = await self.client.get(self.base_url)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "lxml")
                for movie_el in soup.select(".movie-item, .film-item, .movie-card"):
                    title_el = movie_el.select_one("h2, h3, .movie-title, .film-title")
                    title = title_el.get_text(strip=True) if title_el else ""
                    if not title:
                        continue

                    poster_el = movie_el.select_one("img")
                    poster_url = poster_el.get("src", "") if poster_el else ""

                    genre_el = movie_el.select_one(".genre, .movie-genre")
                    genre = genre_el.get_text(strip=True) if genre_el else ""

                    movies.append(ScrapedMovie(
                        title=title,
                        title_he=title,
                        genre=genre,
                        poster_url=poster_url,
                    ))
        except Exception as e:
            logger.warning(f"Lev Cinema movies scrape failed: {e}")
        return movies

    async def scrape_screenings(self) -> list[ScrapedScreening]:
        screenings = []
        for branch_id, branch_info in LEV_BRANCHES.items():
            try:
                url = f"{branch_info['url']}/screenings/{branch_id}"
                resp = await self.client.get(url)
                if resp.status_code != 200:
                    continue

                soup = BeautifulSoup(resp.text, "lxml")
                for screening_el in soup.select(".screening-item, .show-item"):
                    title_el = screening_el.select_one(".movie-title, .film-name")
                    movie_title = title_el.get_text(strip=True) if title_el else "Unknown"

                    time_el = screening_el.select_one(".showtime, .time")
                    time_str = time_el.get_text(strip=True) if time_el else ""

                    try:
                        showtime = datetime.strptime(time_str, "%H:%M").replace(
                            year=datetime.now().year,
                            month=datetime.now().month,
                            day=datetime.now().day,
                        )
                    except (ValueError, AttributeError):
                        showtime = datetime.now()

                    screenings.append(ScrapedScreening(
                        movie_title=movie_title,
                        cinema_name=branch_info["name"],
                        city=branch_info["city"],
                        showtime=showtime,
                        format="2D",
                        language="subtitled",
                        ticket_price=45.0,
                        total_seats=150,
                    ))

            except Exception as e:
                logger.warning(f"Lev Cinema branch {branch_id} scrape failed: {e}")

        return screenings
