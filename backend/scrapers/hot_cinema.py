"""
Scraper for Hot Cinema chain.
"""
import logging
from datetime import datetime
from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, ScrapedMovie, ScrapedScreening

logger = logging.getLogger(__name__)

HOT_CINEMA_BRANCHES = {
    "1": {"name": "Hot Cinema Kfar Saba", "name_he": "הוט סינמה כפר סבא", "city": "Kfar Saba", "city_he": "כפר סבא"},
    "2": {"name": "Hot Cinema Kiryat Ono", "name_he": "הוט סינמה קריית אונו", "city": "Kiryat Ono", "city_he": "קריית אונו"},
    "3": {"name": "Hot Cinema Haifa", "name_he": "הוט סינמה חיפה", "city": "Haifa", "city_he": "חיפה"},
    "4": {"name": "Hot Cinema Ashkelon", "name_he": "הוט סינמה אשקלון", "city": "Ashkelon", "city_he": "אשקלון"},
    "5": {"name": "Hot Cinema Herzliya", "name_he": "הוט סינמה הרצליה", "city": "Herzliya", "city_he": "הרצליה"},
    "6": {"name": "Hot Cinema Rehovot", "name_he": "הוט סינמה רחובות", "city": "Rehovot", "city_he": "רחובות"},
    "7": {"name": "Hot Cinema Petah Tikva", "name_he": "הוט סינמה פתח תקווה", "city": "Petah Tikva", "city_he": "פתח תקווה"},
    "8": {"name": "Hot Cinema Eilat", "name_he": "הוט סינמה אילת", "city": "Eilat", "city_he": "אילת"},
}


class HotCinemaScraper(BaseScraper):

    @property
    def chain_name(self) -> str:
        return "Hot Cinema"

    @property
    def chain_name_he(self) -> str:
        return "הוט סינמה"

    @property
    def base_url(self) -> str:
        return "https://hotcinema.co.il"

    async def scrape_movies(self) -> list[ScrapedMovie]:
        movies = []
        try:
            url = f"{self.base_url}/api/movies"
            resp = await self.client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                for item in data if isinstance(data, list) else data.get("movies", []):
                    movie = ScrapedMovie(
                        title=item.get("titleEn", item.get("title", "")),
                        title_he=item.get("title", ""),
                        genre=item.get("genre", ""),
                        duration_minutes=item.get("duration", 0),
                        release_date=item.get("releaseDate", ""),
                        poster_url=item.get("poster", ""),
                        rating=item.get("rating", ""),
                        director=item.get("director", ""),
                    )
                    movies.append(movie)
        except Exception as e:
            logger.warning(f"Hot Cinema movies scrape failed: {e}")
        return movies

    async def scrape_screenings(self) -> list[ScrapedScreening]:
        screenings = []
        today = datetime.now().strftime("%Y-%m-%d")

        for branch_id, branch_info in HOT_CINEMA_BRANCHES.items():
            try:
                url = f"{self.base_url}/api/screenings?cinema={branch_id}&date={today}"
                resp = await self.client.get(url)
                if resp.status_code != 200:
                    continue

                data = resp.json()
                items = data if isinstance(data, list) else data.get("screenings", [])

                for item in items:
                    try:
                        showtime = datetime.fromisoformat(item.get("datetime", ""))
                    except (ValueError, AttributeError):
                        showtime = datetime.now()

                    screening = ScrapedScreening(
                        movie_title=item.get("movieTitle", item.get("title", "Unknown")),
                        cinema_name=branch_info["name"],
                        city=branch_info["city"],
                        showtime=showtime,
                        hall=item.get("hall", ""),
                        format=item.get("format", "2D"),
                        language=item.get("language", "subtitled"),
                        ticket_price=float(item.get("price", 39.0)),
                        total_seats=int(item.get("totalSeats", 200)),
                        tickets_sold=int(item.get("soldSeats", 0)),
                    )
                    screening.revenue = screening.tickets_sold * screening.ticket_price
                    screenings.append(screening)

            except Exception as e:
                logger.warning(f"Hot Cinema branch {branch_id} scrape failed: {e}")

        return screenings
