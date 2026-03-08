"""
Scraper for Globus Max cinema chain.
"""
import logging
from datetime import datetime

from scrapers.base import BaseScraper, ScrapedMovie, ScrapedScreening

logger = logging.getLogger(__name__)

GLOBUS_MAX_BRANCHES = {
    "1": {"name": "Globus Max Rishon LeZion", "name_he": "גלובוס מקס ראשון לציון", "city": "Rishon LeZion", "city_he": "ראשון לציון"},
    "2": {"name": "Globus Max Holon", "name_he": "גלובוס מקס חולון", "city": "Holon", "city_he": "חולון"},
    "3": {"name": "Globus Max Hadera", "name_he": "גלובוס מקס חדרה", "city": "Hadera", "city_he": "חדרה"},
    "4": {"name": "Globus Max Natanya", "name_he": "גלובוס מקס נתניה", "city": "Netanya", "city_he": "נתניה"},
}


class GlobusMaxScraper(BaseScraper):

    @property
    def chain_name(self) -> str:
        return "Globus Max"

    @property
    def chain_name_he(self) -> str:
        return "גלובוס מקס"

    @property
    def base_url(self) -> str:
        return "https://www.globusmax.co.il"

    async def scrape_movies(self) -> list[ScrapedMovie]:
        movies = []
        try:
            resp = await self.client.get(f"{self.base_url}/page/movies")
            if resp.status_code == 200:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(resp.text, "lxml")
                for card in soup.select(".movie-card, .film-item, .movie"):
                    title_el = card.select_one("h2, h3, .title, .movie-name")
                    title = title_el.get_text(strip=True) if title_el else ""
                    if not title:
                        continue
                    poster_el = card.select_one("img")
                    poster = poster_el.get("src", "") if poster_el else ""
                    movies.append(ScrapedMovie(
                        title=title,
                        title_he=title,
                        poster_url=poster,
                    ))
        except Exception as e:
            logger.warning(f"Globus Max movies scrape failed: {e}")
        return movies

    async def scrape_screenings(self) -> list[ScrapedScreening]:
        screenings = []
        today = datetime.now().strftime("%Y-%m-%d")

        for branch_id, branch_info in GLOBUS_MAX_BRANCHES.items():
            try:
                url = f"{self.base_url}/api/screenings/{branch_id}?date={today}"
                resp = await self.client.get(url)
                if resp.status_code != 200:
                    continue

                data = resp.json()
                items = data if isinstance(data, list) else data.get("screenings", [])

                for item in items:
                    try:
                        showtime = datetime.fromisoformat(item.get("startTime", ""))
                    except (ValueError, AttributeError):
                        showtime = datetime.now()

                    screenings.append(ScrapedScreening(
                        movie_title=item.get("movieName", "Unknown"),
                        cinema_name=branch_info["name"],
                        city=branch_info["city"],
                        showtime=showtime,
                        hall=item.get("hall", ""),
                        format=item.get("screenType", "2D"),
                        language="subtitled",
                        ticket_price=float(item.get("price", 40.0)),
                        total_seats=int(item.get("capacity", 200)),
                        tickets_sold=int(item.get("ticketsSold", 0)),
                    ))
                    screenings[-1].revenue = screenings[-1].tickets_sold * screenings[-1].ticket_price

            except Exception as e:
                logger.warning(f"Globus Max branch {branch_id} scrape failed: {e}")

        return screenings
