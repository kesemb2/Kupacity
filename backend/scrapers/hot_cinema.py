"""
Scraper for Hot Cinema chain using Playwright (headless browser).

No API needed - scrapes directly from the rendered website pages.

URL patterns:
- Theater page:  https://hotcinema.co.il/theater/{id}/{slug}
- Movie page:    https://hotcinema.co.il/movie/{id}/{slug}

Schedule:
- Weekly:  scrape_movies()         - full movie catalog from all theaters
- Daily:   scrape_screenings()     - screening schedule for next 7 days
- 5 hours: scrape_ticket_updates() - ticket availability for today's screenings
"""
import logging
import asyncio
import re
from datetime import datetime, timedelta
from playwright.async_api import async_playwright, Page, Browser

from scrapers.base import BaseScraper, ScrapedMovie, ScrapedScreening

logger = logging.getLogger(__name__)

# Updated branch list based on actual site data
HOT_CINEMA_BRANCHES = {
    "1": {"name": "Hot Cinema Modi'in", "name_he": "הוט סינמה מודיעין", "city": "Modi'in", "city_he": "מודיעין", "slug": "מודיעין"},
    "2": {"name": "Hot Cinema Kfar Saba", "name_he": "הוט סינמה כפר סבא", "city": "Kfar Saba", "city_he": "כפר סבא", "slug": "כפר-סבא"},
    "3": {"name": "Hot Cinema Petah Tikva", "name_he": "הוט סינמה פתח תקווה", "city": "Petah Tikva", "city_he": "פתח תקווה", "slug": "פתח-תקווה"},
    "4": {"name": "Hot Cinema Rehovot", "name_he": "הוט סינמה רחובות", "city": "Rehovot", "city_he": "רחובות", "slug": "רחובות"},
    "5": {"name": "Hot Cinema Haifa", "name_he": "הוט סינמה חיפה", "city": "Haifa", "city_he": "חיפה", "slug": "חיפה"},
    "6": {"name": "Hot Cinema Kiryon", "name_he": "הוט סינמה קריון", "city": "Kiryon", "city_he": "קריון", "slug": "קריון"},
    "7": {"name": "Hot Cinema Karmiel", "name_he": "הוט סינמה כרמיאל", "city": "Karmiel", "city_he": "כרמיאל", "slug": "כרמיאל"},
    "8": {"name": "Hot Cinema Nahariya", "name_he": "הוט סינמה נהריה", "city": "Nahariya", "city_he": "נהריה", "slug": "נהריה"},
    "9": {"name": "Hot Cinema Ashkelon", "name_he": "הוט סינמה אשקלון", "city": "Ashkelon", "city_he": "אשקלון", "slug": "אשקלון"},
    "10": {"name": "Hot Cinema Ashdod", "name_he": "הוט סינמה אשדוד", "city": "Ashdod", "city_he": "אשדוד", "slug": "אשדוד"},
}

BASE_URL = "https://hotcinema.co.il"


class HotCinemaScraper(BaseScraper):

    @property
    def chain_name(self) -> str:
        return "Hot Cinema"

    @property
    def chain_name_he(self) -> str:
        return "הוט סינמה"

    @property
    def base_url(self) -> str:
        return BASE_URL

    async def _launch_browser(self) -> tuple:
        """Launch Playwright browser and return (playwright, browser)."""
        pw = await async_playwright().start()
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox"],
        )
        return pw, browser

    async def _new_page(self, browser: Browser) -> Page:
        """Create a new page with realistic browser settings."""
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            locale="he-IL",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        page = await context.new_page()
        return page

    async def _scrape_theater_page(self, page: Page, branch_id: str,
                                    branch_info: dict) -> tuple[list[ScrapedMovie], list[ScrapedScreening]]:
        """Scrape a single theater page for movies and screenings."""
        movies = []
        screenings = []

        url = f"{BASE_URL}/theater/{branch_id}/{branch_info['slug']}"
        try:
            await page.goto(url, wait_until="networkidle", timeout=30000)
            await page.wait_for_timeout(2000)  # let dynamic content load

            # Try to find movie elements on the page
            # Look for common patterns: movie cards, screening containers
            movie_elements = await page.query_selector_all(
                '[class*="movie"], [class*="Movie"], [class*="film"], [class*="Film"], '
                '[data-movie], article, .card, [class*="item"]'
            )

            for elem in movie_elements:
                try:
                    # Extract movie title
                    title_el = await elem.query_selector(
                        'h2, h3, h4, [class*="title"], [class*="name"], [class*="Title"], [class*="Name"]'
                    )
                    if not title_el:
                        continue
                    title = (await title_el.inner_text()).strip()
                    if not title or len(title) < 2:
                        continue

                    # Extract poster image
                    poster_url = ""
                    img_el = await elem.query_selector('img')
                    if img_el:
                        poster_url = await img_el.get_attribute('src') or ""
                        if poster_url and not poster_url.startswith('http'):
                            poster_url = f"{BASE_URL}{poster_url}"

                    # Extract movie link to get movie ID
                    link_el = await elem.query_selector('a[href*="/movie/"]')
                    movie_link = ""
                    if link_el:
                        movie_link = await link_el.get_attribute('href') or ""

                    movie = ScrapedMovie(
                        title=title,
                        title_he=title,
                        poster_url=poster_url,
                    )
                    movies.append(movie)

                    # Extract screenings (showtimes) for this movie
                    showtime_elements = await elem.query_selector_all(
                        '[class*="showtime"], [class*="time"], [class*="screening"], '
                        '[class*="Showtime"], [class*="Time"], [class*="Screening"], '
                        'button[class*="time"], a[class*="time"]'
                    )

                    for st_el in showtime_elements:
                        try:
                            time_text = (await st_el.inner_text()).strip()
                            # Parse time like "19:30", "21:00"
                            time_match = re.search(r'(\d{1,2}):(\d{2})', time_text)
                            if not time_match:
                                continue

                            hour, minute = int(time_match.group(1)), int(time_match.group(2))
                            showtime = datetime.now().replace(
                                hour=hour, minute=minute, second=0, microsecond=0
                            )
                            # If time already passed today, it might be for tomorrow
                            if showtime < datetime.now():
                                showtime += timedelta(days=1)

                            # Try to extract format info
                            format_text = time_text.upper()
                            screen_format = "2D"
                            if "IMAX" in format_text:
                                screen_format = "IMAX"
                            elif "4DX" in format_text:
                                screen_format = "4DX"
                            elif "3D" in format_text:
                                screen_format = "3D"
                            elif "SCREENX" in format_text:
                                screen_format = "ScreenX"

                            # Check for hall info
                            hall = ""
                            hall_attr = await st_el.get_attribute('data-hall')
                            if hall_attr:
                                hall = hall_attr

                            # Check seat availability from element attributes or nearby text
                            total_seats = 200  # default
                            tickets_sold = 0
                            seats_attr = await st_el.get_attribute('data-seats')
                            if seats_attr:
                                try:
                                    total_seats = int(seats_attr)
                                except ValueError:
                                    pass
                            sold_attr = await st_el.get_attribute('data-sold')
                            if sold_attr:
                                try:
                                    tickets_sold = int(sold_attr)
                                except ValueError:
                                    pass

                            # Check if screening is sold out
                            classes = await st_el.get_attribute('class') or ""
                            el_text = time_text.lower()
                            if 'sold' in classes.lower() or 'disabled' in classes.lower() or 'אזל' in el_text:
                                tickets_sold = total_seats  # sold out

                            screening = ScrapedScreening(
                                movie_title=title,
                                cinema_name=branch_info["name"],
                                city=branch_info["city"],
                                showtime=showtime,
                                hall=hall,
                                format=screen_format,
                                language="subtitled",
                                ticket_price=39.0,
                                total_seats=total_seats,
                                tickets_sold=tickets_sold,
                            )
                            screening.revenue = screening.tickets_sold * screening.ticket_price
                            screenings.append(screening)

                        except Exception as e:
                            logger.debug(f"Showtime parse error: {e}")
                            continue

                except Exception as e:
                    logger.debug(f"Movie element parse error: {e}")
                    continue

        except Exception as e:
            logger.warning(f"Hot Cinema theater {branch_id} page scrape failed: {e}")

        return movies, screenings

    async def _scrape_movie_detail(self, page: Page, movie_path: str) -> ScrapedMovie | None:
        """Scrape detailed info from a movie's dedicated page."""
        try:
            url = f"{BASE_URL}{movie_path}" if movie_path.startswith('/') else movie_path
            await page.goto(url, wait_until="networkidle", timeout=20000)
            await page.wait_for_timeout(1500)

            title = ""
            title_el = await page.query_selector('h1, [class*="movieTitle"], [class*="MovieTitle"]')
            if title_el:
                title = (await title_el.inner_text()).strip()

            genre = ""
            genre_el = await page.query_selector('[class*="genre"], [class*="Genre"]')
            if genre_el:
                genre = (await genre_el.inner_text()).strip()

            duration = 0
            duration_el = await page.query_selector('[class*="duration"], [class*="Duration"], [class*="length"]')
            if duration_el:
                dur_text = await duration_el.inner_text()
                dur_match = re.search(r'(\d+)', dur_text)
                if dur_match:
                    duration = int(dur_match.group(1))

            director = ""
            director_el = await page.query_selector('[class*="director"], [class*="Director"]')
            if director_el:
                director = (await director_el.inner_text()).strip()

            poster_url = ""
            poster_el = await page.query_selector('[class*="poster"] img, [class*="Poster"] img, .movie-image img')
            if poster_el:
                poster_url = await poster_el.get_attribute('src') or ""
                if poster_url and not poster_url.startswith('http'):
                    poster_url = f"{BASE_URL}{poster_url}"

            if title:
                return ScrapedMovie(
                    title=title,
                    title_he=title,
                    genre=genre,
                    duration_minutes=duration,
                    poster_url=poster_url,
                    director=director,
                )
        except Exception as e:
            logger.warning(f"Movie detail scrape failed for {movie_path}: {e}")

        return None

    async def scrape_movies(self) -> list[ScrapedMovie]:
        """Weekly task: scrape all movies from all theater pages."""
        all_movies = {}
        pw, browser = await self._launch_browser()
        try:
            page = await self._new_page(browser)

            # Scrape movies from each theater page
            for branch_id, branch_info in HOT_CINEMA_BRANCHES.items():
                movies, _ = await self._scrape_theater_page(page, branch_id, branch_info)
                for m in movies:
                    if m.title and m.title not in all_movies:
                        all_movies[m.title] = m

            # Also try the main page for any movies listed there
            try:
                await page.goto(BASE_URL, wait_until="networkidle", timeout=30000)
                await page.wait_for_timeout(2000)

                # Find all movie links on the homepage
                movie_links = await page.query_selector_all('a[href*="/movie/"]')
                movie_paths = set()
                for link in movie_links:
                    href = await link.get_attribute('href')
                    if href and '/movie/' in href:
                        movie_paths.add(href)

                # Scrape details for each movie
                for path in list(movie_paths)[:30]:  # limit to 30 movies
                    movie = await self._scrape_movie_detail(page, path)
                    if movie and movie.title and movie.title not in all_movies:
                        all_movies[movie.title] = movie
                    await page.wait_for_timeout(500)  # be polite

            except Exception as e:
                logger.warning(f"Hot Cinema homepage scrape failed: {e}")

            await page.close()
        finally:
            await browser.close()
            await pw.stop()

        result = list(all_movies.values())
        logger.info(f"[Hot Cinema] Scraped {len(result)} unique movies")
        return result

    async def scrape_screenings(self) -> list[ScrapedScreening]:
        """Daily task: scrape screenings from all theater pages."""
        all_screenings = []
        pw, browser = await self._launch_browser()
        try:
            page = await self._new_page(browser)

            for branch_id, branch_info in HOT_CINEMA_BRANCHES.items():
                # Scrape today's page
                _, screenings = await self._scrape_theater_page(page, branch_id, branch_info)
                all_screenings.extend(screenings)

                # Try to click through dates for the next 7 days
                try:
                    date_buttons = await page.query_selector_all(
                        '[class*="date"], [class*="Date"], [class*="day"], [class*="Day"], '
                        '[data-date], button[class*="calendar"]'
                    )
                    for btn in date_buttons[1:7]:  # skip first (today, already scraped)
                        try:
                            await btn.click()
                            await page.wait_for_timeout(2000)
                            _, day_screenings = await self._scrape_theater_page(
                                page, branch_id, branch_info
                            )
                            all_screenings.extend(day_screenings)
                        except Exception:
                            continue
                except Exception as e:
                    logger.debug(f"Date navigation failed for branch {branch_id}: {e}")

                await page.wait_for_timeout(1000)  # rate limiting

            await page.close()
        finally:
            await browser.close()
            await pw.stop()

        logger.info(f"[Hot Cinema] Scraped {len(all_screenings)} screenings")
        return all_screenings

    async def scrape_ticket_updates(self) -> list[ScrapedScreening]:
        """Every 5 hours: scrape today's screenings to get updated availability."""
        all_screenings = []
        pw, browser = await self._launch_browser()
        try:
            page = await self._new_page(browser)

            for branch_id, branch_info in HOT_CINEMA_BRANCHES.items():
                _, screenings = await self._scrape_theater_page(page, branch_id, branch_info)
                all_screenings.extend(screenings)
                await page.wait_for_timeout(1000)

            await page.close()
        finally:
            await browser.close()
            await pw.stop()

        logger.info(f"[Hot Cinema] Ticket update: {len(all_screenings)} screenings")
        return all_screenings
