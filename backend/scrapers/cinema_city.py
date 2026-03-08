"""
Scraper for Cinema City / Yes Planet
Cinema City API exposes data via their internal REST API.
"""
import logging
from datetime import datetime
from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, ScrapedMovie, ScrapedScreening

logger = logging.getLogger(__name__)

# Cinema City branches with cities
CINEMA_CITY_BRANCHES = {
    "1058": {"name": "Yes Planet Rishon LeZion", "name_he": "יס פלאנט ראשון לציון", "city": "Rishon LeZion", "city_he": "ראשון לציון"},
    "1071": {"name": "Yes Planet Haifa", "name_he": "יס פלאנט חיפה", "city": "Haifa", "city_he": "חיפה"},
    "1072": {"name": "Yes Planet Beer Sheva", "name_he": "יס פלאנט באר שבע", "city": "Beer Sheva", "city_he": "באר שבע"},
    "1073": {"name": "Yes Planet Jerusalem", "name_he": "יס פלאנט ירושלים", "city": "Jerusalem", "city_he": "ירושלים"},
    "1025": {"name": "Cinema City Glilot", "name_he": "סינמה סיטי גלילות", "city": "Ramat HaSharon", "city_he": "רמת השרון"},
    "1070": {"name": "Cinema City Netanya", "name_he": "סינמה סיטי נתניה", "city": "Netanya", "city_he": "נתניה"},
    "1075": {"name": "Cinema City Ashdod", "name_he": "סינמה סיטי אשדוד", "city": "Ashdod", "city_he": "אשדוד"},
}


class CinemaCityScraper(BaseScraper):

    @property
    def chain_name(self) -> str:
        return "Cinema City"

    @property
    def chain_name_he(self) -> str:
        return "סינמה סיטי / יס פלאנט"

    @property
    def base_url(self) -> str:
        return "https://www.cinema-city.co.il"

    async def scrape_movies(self) -> list[ScrapedMovie]:
        movies = []
        try:
            url = f"{self.base_url}/il/data-api-service/v1/quickbook/10104/film-events/in-cinema/1058/at-date/2026-03-08?attr=&lang=he_IL"
            resp = await self.client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                for film in data.get("body", {}).get("films", []):
                    movie = ScrapedMovie(
                        title=film.get("name", ""),
                        title_he=film.get("name", ""),
                        genre=", ".join(film.get("genres", [])) if film.get("genres") else "",
                        duration_minutes=film.get("length", 0),
                        release_date=film.get("releaseDate", ""),
                        poster_url=film.get("posterLink", ""),
                        rating=film.get("rating", ""),
                        director=film.get("director", ""),
                    )
                    movies.append(movie)
        except Exception as e:
            logger.warning(f"Cinema City movies scrape failed: {e}")
        return movies

    async def scrape_screenings(self) -> list[ScrapedScreening]:
        screenings = []
        today = datetime.now().strftime("%Y-%m-%d")

        for branch_id, branch_info in CINEMA_CITY_BRANCHES.items():
            try:
                url = (
                    f"{self.base_url}/il/data-api-service/v1/quickbook/10104/"
                    f"film-events/in-cinema/{branch_id}/at-date/{today}"
                    f"?attr=&lang=he_IL"
                )
                resp = await self.client.get(url)
                if resp.status_code != 200:
                    continue

                data = resp.json()
                films_map = {}
                for film in data.get("body", {}).get("films", []):
                    films_map[film.get("id")] = film.get("name", "Unknown")

                for event in data.get("body", {}).get("events", []):
                    film_id = event.get("filmId")
                    movie_title = films_map.get(film_id, "Unknown")

                    showtime_str = event.get("eventDateTime", "")
                    try:
                        showtime = datetime.fromisoformat(showtime_str.replace("Z", "+00:00"))
                    except (ValueError, AttributeError):
                        showtime = datetime.now()

                    attrs = event.get("attributeIds", [])
                    fmt = "2D"
                    if "imax" in str(attrs).lower():
                        fmt = "IMAX"
                    elif "3d" in str(attrs).lower():
                        fmt = "3D"
                    elif "4dx" in str(attrs).lower():
                        fmt = "4DX"
                    elif "screenx" in str(attrs).lower():
                        fmt = "ScreenX"

                    screening = ScrapedScreening(
                        movie_title=movie_title,
                        cinema_name=branch_info["name"],
                        city=branch_info["city"],
                        showtime=showtime,
                        hall=event.get("auditorium", ""),
                        format=fmt,
                        language="subtitled",
                        ticket_price=event.get("priceInCents", 0) / 100 if event.get("priceInCents") else 42.0,
                        total_seats=event.get("seatsAvailable", 0) + event.get("soldSeats", 0),
                        tickets_sold=event.get("soldSeats", 0),
                    )
                    screening.revenue = screening.tickets_sold * screening.ticket_price
                    screenings.append(screening)

            except Exception as e:
                logger.warning(f"Cinema City branch {branch_id} scrape failed: {e}")
                continue

        return screenings
