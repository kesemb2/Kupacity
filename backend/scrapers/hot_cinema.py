"""
Scraper for Hot Cinema chain using Playwright (headless browser).

No API needed - scrapes directly from the rendered website pages,
including entering each screening's seat selection page to count
occupied vs available seats.

URL patterns:
- Theater page:  https://hotcinema.co.il/theater/{id}/{slug}
- Movie page:    https://hotcinema.co.il/movie/{id}/{slug}
- Seat map:      https://tickets.hotcinema.co.il/site/{siteId}/seats

Schedule:
- Weekly:  scrape_movies()         - full movie catalog from all theaters
- Daily:   scrape_screenings()     - screening schedule for next 7 days
- 5 hours: scrape_ticket_updates() - enter each screening's seat page and count sold seats

Anti-detection:
- Rotates real-browser User-Agent strings each session
- Randomised delays (2-7 s) between navigations
- Human-like mouse movement + scroll before data extraction
- playwright-stealth patches (webdriver, chrome.runtime, etc.)
- Optional proxy via SCRAPER_PROXY_SERVER env var
"""
import asyncio
import logging
import os
import random
import re
from datetime import datetime, timedelta
from playwright.async_api import async_playwright, Page, Browser
from playwright_stealth import stealth_async

from scrapers.base import BaseScraper, ScrapedMovie, ScrapedScreening

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Real-browser User-Agent pool (Chrome / Firefox / Edge on Windows & Mac)
# ---------------------------------------------------------------------------
_USER_AGENTS = [
    # Chrome 131 – Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    # Chrome 130 – Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    # Chrome 129 – Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    # Firefox 132 – Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
    # Firefox 131 – Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:131.0) Gecko/20100101 Firefox/131.0",
    # Edge 131 – Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
    # Chrome 131 – Linux
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    # Safari 17 – Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
]

# Branch list based on actual site data
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
TICKETS_URL = "https://tickets.hotcinema.co.il"


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

    # ------------------------------------------------------------------
    # Anti-detection helpers
    # ------------------------------------------------------------------

    @staticmethod
    async def _human_delay(lo: float = 2.0, hi: float = 7.0):
        """Sleep for a random duration to mimic human pacing."""
        await asyncio.sleep(random.uniform(lo, hi))

    @staticmethod
    async def _simulate_human(page: Page):
        """Move mouse to random coordinates and scroll slightly."""
        # Random mouse movement (3-6 points)
        for _ in range(random.randint(3, 6)):
            x = random.randint(100, 1800)
            y = random.randint(100, 900)
            await page.mouse.move(x, y, steps=random.randint(5, 15))
            await asyncio.sleep(random.uniform(0.05, 0.2))

        # Gentle scroll down then back up
        scroll_y = random.randint(150, 500)
        await page.mouse.wheel(0, scroll_y)
        await asyncio.sleep(random.uniform(0.3, 0.8))
        await page.mouse.wheel(0, -random.randint(50, scroll_y))
        await asyncio.sleep(random.uniform(0.2, 0.5))

    # ------------------------------------------------------------------
    # Browser / page lifecycle
    # ------------------------------------------------------------------

    async def _launch_browser(self) -> tuple:
        """Launch Playwright Chromium with stealth flags and optional proxy.

        To route traffic through a proxy, set the env var:
            SCRAPER_PROXY_SERVER=http://user:pass@host:port
        """
        pw = await async_playwright().start()

        launch_kwargs: dict = {
            "headless": True,
            "args": [
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
                "--window-size=1920,1080",
            ],
        }

        # ---- Proxy configuration (optional) ----------------------------
        proxy_server = os.environ.get("SCRAPER_PROXY_SERVER")
        if proxy_server:
            proxy_cfg: dict = {"server": proxy_server}
            proxy_user = os.environ.get("SCRAPER_PROXY_USERNAME")
            proxy_pass = os.environ.get("SCRAPER_PROXY_PASSWORD")
            if proxy_user:
                proxy_cfg["username"] = proxy_user
            if proxy_pass:
                proxy_cfg["password"] = proxy_pass
            launch_kwargs["proxy"] = proxy_cfg
            logger.info(f"[Hot Cinema] Using proxy: {proxy_server}")
        # ----------------------------------------------------------------

        browser = await pw.chromium.launch(**launch_kwargs)
        return pw, browser

    async def _new_page(self, browser: Browser) -> Page:
        """Create a new browser context + page with a random UA and stealth."""
        ua = random.choice(_USER_AGENTS)
        logger.debug(f"[Hot Cinema] Selected UA: {ua}")

        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
            timezone_id="Asia/Jerusalem",
            user_agent=ua,
            java_script_enabled=True,
            color_scheme="light",
            # Extra realistic headers
            extra_http_headers={
                "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
                "Accept-Encoding": "gzip, deflate, br",
                "Sec-CH-UA": '"Chromium";v="131", "Not_A Brand";v="24"',
                "Sec-CH-UA-Mobile": "?0",
                "Sec-CH-UA-Platform": '"Windows"',
            },
        )

        page = await context.new_page()
        await stealth_async(page)
        return page

    async def _goto_with_stealth(self, page: Page, url: str,
                                  timeout: int = 30000) -> object:
        """Navigate to *url* with human-like delay + mouse jitter."""
        await self._human_delay()
        resp = await page.goto(url, wait_until="networkidle", timeout=timeout)

        # Log the actual response for debugging 403s
        if resp and resp.status >= 400:
            body_snippet = ""
            try:
                body_snippet = (await resp.text())[:300]
            except Exception:
                pass
            logger.warning(
                f"[Hot Cinema] HTTP {resp.status} for {url}"
                + (f" | body: {body_snippet}" if body_snippet else "")
            )

        await self._simulate_human(page)
        return resp

    # ------------------------------------------------------------------
    # Seat counting: enter the booking/seats page and count occupied seats
    # ------------------------------------------------------------------

    async def _count_seats_on_page(self, page: Page) -> tuple[int, int]:
        """
        On a seat selection page, count total seats and occupied seats.
        Returns (total_seats, tickets_sold).

        Seat maps typically show seats as SVG elements, divs, or buttons.
        Available seats are clickable/selectable, occupied seats have a
        different class/style (e.g. 'sold', 'occupied', 'taken', 'unavailable').
        """
        total = 0
        sold = 0

        # Strategy 1: Look for individual seat elements with status indicators
        seat_selectors = [
            'svg [class*="seat"], svg [data-seat], svg rect[class], svg circle[class]',
            '[class*="seat"]:not([class*="seatmap"]):not([class*="seating"]):not([class*="seats-"]), '
            '[data-seat-id], [data-seat], [class*="Seat"]:not([class*="Seatmap"])',
            '.seat, .chair, [role="button"][class*="seat"]',
        ]

        for selector in seat_selectors:
            try:
                seats = await page.query_selector_all(selector)
                if len(seats) < 5:
                    continue

                total = len(seats)
                for seat in seats:
                    classes = (await seat.get_attribute('class') or "").lower()
                    data_status = (await seat.get_attribute('data-status') or "").lower()
                    aria_disabled = (await seat.get_attribute('aria-disabled') or "").lower()
                    style = (await seat.get_attribute('style') or "").lower()

                    is_sold = any([
                        'sold' in classes,
                        'occupied' in classes,
                        'taken' in classes,
                        'unavailable' in classes,
                        'reserved' in classes,
                        'disabled' in classes,
                        'booked' in classes,
                        'תפוס' in classes,
                        data_status in ('sold', 'occupied', 'taken', 'unavailable', 'reserved', 'booked'),
                        aria_disabled == 'true',
                        'pointer-events: none' in style and 'opacity' in style,
                    ])

                    fill = (await seat.get_attribute('fill') or "").lower()
                    if fill and not is_sold:
                        sold_colors = ['#ccc', '#ddd', '#999', '#888', '#666', 'gray', 'grey',
                                       '#ff0000', 'red', '#c0c0c0', '#808080']
                        if any(c in fill for c in sold_colors):
                            is_sold = True

                    if is_sold:
                        sold += 1

                if total > 0:
                    break
            except Exception as e:
                logger.debug(f"Seat selector '{selector}' failed: {e}")
                continue

        # Strategy 2: text-based seat indicator
        if total == 0:
            try:
                body_text = await page.inner_text('body')
                remaining_match = re.search(r'נותרו\s+(\d+)\s+מקומות', body_text)
                if remaining_match:
                    available = int(remaining_match.group(1))
                    total = available
                    sold = 0

                ratio_match = re.search(r'(\d+)\s*/\s*(\d+)', body_text)
                if ratio_match:
                    sold = int(ratio_match.group(1))
                    total = int(ratio_match.group(2))
            except Exception:
                pass

        return total, sold

    async def _get_seat_count_for_screening(self, page: Page, screening_url: str) -> tuple[int, int]:
        """Navigate to a screening's seat selection page and count seats."""
        try:
            resp = await self._goto_with_stealth(page, screening_url)
            if resp and resp.status >= 400:
                return 0, 0
            await page.wait_for_timeout(3000)
            total, sold = await self._count_seats_on_page(page)
            return total, sold
        except Exception as e:
            logger.warning(f"Seat count failed for {screening_url}: {e}")
            return 0, 0

    # ------------------------------------------------------------------
    # Theater page scraping: get movies and showtimes with booking links
    # ------------------------------------------------------------------

    async def _scrape_theater_page(self, page: Page, branch_id: str,
                                    branch_info: dict) -> tuple[list[ScrapedMovie], list[dict]]:
        """
        Scrape a single theater page for movies and screenings.
        Returns movies and screening_infos (dicts with booking URLs for later seat counting).
        """
        movies = []
        screening_infos = []

        url = f"{BASE_URL}/theater/{branch_id}/{branch_info['slug']}"
        try:
            resp = await self._goto_with_stealth(page, url)
            if resp and resp.status >= 400:
                return movies, screening_infos

            # Find movie elements
            movie_elements = await page.query_selector_all(
                '[class*="movie"], [class*="Movie"], [class*="film"], [class*="Film"], '
                '[data-movie], article, .card, [class*="item"]'
            )

            for elem in movie_elements:
                try:
                    title_el = await elem.query_selector(
                        'h2, h3, h4, [class*="title"], [class*="name"], [class*="Title"], [class*="Name"]'
                    )
                    if not title_el:
                        continue
                    title = (await title_el.inner_text()).strip()
                    if not title or len(title) < 2:
                        continue

                    poster_url = ""
                    img_el = await elem.query_selector('img')
                    if img_el:
                        poster_url = await img_el.get_attribute('src') or ""
                        if poster_url and not poster_url.startswith('http'):
                            poster_url = f"{BASE_URL}{poster_url}"

                    movie = ScrapedMovie(
                        title=title,
                        title_he=title,
                        poster_url=poster_url,
                    )
                    movies.append(movie)

                    showtime_elements = await elem.query_selector_all(
                        'a[href*="tickets"], a[href*="booking"], a[href*="order"], '
                        'a[href*="seats"], a[href*="site"], '
                        '[class*="showtime"], [class*="time"], [class*="screening"], '
                        '[class*="Showtime"], [class*="Time"], [class*="Screening"], '
                        'button[class*="time"], a[class*="time"]'
                    )

                    for st_el in showtime_elements:
                        try:
                            time_text = (await st_el.inner_text()).strip()
                            time_match = re.search(r'(\d{1,2}):(\d{2})', time_text)
                            if not time_match:
                                continue

                            hour, minute = int(time_match.group(1)), int(time_match.group(2))
                            showtime = datetime.now().replace(
                                hour=hour, minute=minute, second=0, microsecond=0
                            )
                            if showtime < datetime.now():
                                showtime += timedelta(days=1)

                            format_text = time_text.upper()
                            parent_text = ""
                            try:
                                parent = await st_el.evaluate_handle('el => el.closest("[class*=format], [class*=Format], [class*=type], [class*=Type]")')
                                if parent:
                                    parent_text = (await parent.inner_text() if hasattr(parent, 'inner_text') else "").upper()
                            except Exception:
                                pass
                            combined_text = format_text + " " + parent_text

                            screen_format = "2D"
                            if "IMAX" in combined_text:
                                screen_format = "IMAX"
                            elif "4DX" in combined_text:
                                screen_format = "4DX"
                            elif "3D" in combined_text:
                                screen_format = "3D"
                            elif "SCREENX" in combined_text:
                                screen_format = "ScreenX"

                            hall = ""
                            hall_attr = await st_el.get_attribute('data-hall')
                            if hall_attr:
                                hall = hall_attr

                            booking_url = ""
                            href = await st_el.get_attribute('href')
                            if href:
                                if href.startswith('http'):
                                    booking_url = href
                                elif href.startswith('/'):
                                    booking_url = f"{TICKETS_URL}{href}" if 'site' in href or 'seats' in href else f"{BASE_URL}{href}"
                            else:
                                data_url = await st_el.get_attribute('data-url') or await st_el.get_attribute('data-href') or ""
                                if data_url:
                                    booking_url = data_url if data_url.startswith('http') else f"{TICKETS_URL}{data_url}"

                            screening_infos.append({
                                "movie_title": title,
                                "cinema_name": branch_info["name"],
                                "city": branch_info["city"],
                                "showtime": showtime,
                                "hall": hall,
                                "format": screen_format,
                                "booking_url": booking_url,
                            })

                        except Exception as e:
                            logger.debug(f"Showtime parse error: {e}")
                            continue

                except Exception as e:
                    logger.debug(f"Movie element parse error: {e}")
                    continue

        except Exception as e:
            logger.warning(f"Hot Cinema theater {branch_id} page scrape failed: {e}")

        return movies, screening_infos

    async def _scrape_movie_detail(self, page: Page, movie_path: str) -> ScrapedMovie | None:
        """Scrape detailed info from a movie's dedicated page."""
        try:
            url = f"{BASE_URL}{movie_path}" if movie_path.startswith('/') else movie_path
            resp = await self._goto_with_stealth(page, url, timeout=20000)
            if resp and resp.status >= 400:
                return None

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
                    title=title, title_he=title, genre=genre,
                    duration_minutes=duration, poster_url=poster_url, director=director,
                )
        except Exception as e:
            logger.warning(f"Movie detail scrape failed for {movie_path}: {e}")
        return None

    # ------------------------------------------------------------------
    # Public scrape methods (called by the scheduler)
    # ------------------------------------------------------------------

    async def scrape_movies(self) -> list[ScrapedMovie]:
        """Weekly: scrape all movies from all theater pages + movie detail pages."""
        all_movies = {}
        pw, browser = await self._launch_browser()
        try:
            page = await self._new_page(browser)

            for branch_id, branch_info in HOT_CINEMA_BRANCHES.items():
                movies, _ = await self._scrape_theater_page(page, branch_id, branch_info)
                for m in movies:
                    if m.title and m.title not in all_movies:
                        all_movies[m.title] = m

            # Also scrape movie detail pages from the homepage
            try:
                await self._goto_with_stealth(page, BASE_URL)
                movie_links = await page.query_selector_all('a[href*="/movie/"]')
                movie_paths = set()
                for link in movie_links:
                    href = await link.get_attribute('href')
                    if href and '/movie/' in href:
                        movie_paths.add(href)
                for path in list(movie_paths)[:30]:
                    movie = await self._scrape_movie_detail(page, path)
                    if movie and movie.title and movie.title not in all_movies:
                        all_movies[movie.title] = movie
            except Exception as e:
                logger.warning(f"Hot Cinema homepage scrape failed: {e}")

            await page.close()
        finally:
            await browser.close()
            await pw.stop()

        result = list(all_movies.values())
        if not result:
            logger.warning("[Hot Cinema] No movies found - site may be unreachable or structure changed")
        logger.info(f"[Hot Cinema] Scraped {len(result)} unique movies")
        return result

    async def scrape_screenings(self) -> list[ScrapedScreening]:
        """Daily: scrape screenings from all theater pages for next 7 days."""
        all_screenings = []
        pw, browser = await self._launch_browser()
        try:
            page = await self._new_page(browser)

            for branch_id, branch_info in HOT_CINEMA_BRANCHES.items():
                _, screening_infos = await self._scrape_theater_page(page, branch_id, branch_info)

                # Try clicking date buttons for next 7 days
                try:
                    date_buttons = await page.query_selector_all(
                        '[class*="date"], [class*="Date"], [class*="day"], [class*="Day"], '
                        '[data-date], button[class*="calendar"]'
                    )
                    for btn in date_buttons[1:7]:
                        try:
                            await btn.click()
                            await self._human_delay(1.5, 4.0)
                            _, day_infos = await self._scrape_theater_page(page, branch_id, branch_info)
                            screening_infos.extend(day_infos)
                        except Exception:
                            continue
                except Exception:
                    pass

                for info in screening_infos:
                    screening = ScrapedScreening(
                        movie_title=info["movie_title"],
                        cinema_name=info["cinema_name"],
                        city=info["city"],
                        showtime=info["showtime"],
                        hall=info["hall"],
                        format=info["format"],
                        language="subtitled",
                        ticket_price=39.0,
                        total_seats=200,
                        tickets_sold=0,
                    )
                    screening.revenue = 0
                    all_screenings.append(screening)

            await page.close()
        finally:
            await browser.close()
            await pw.stop()

        logger.info(f"[Hot Cinema] Daily scrape: {len(all_screenings)} screenings")
        return all_screenings

    async def scrape_ticket_updates(self) -> list[ScrapedScreening]:
        """
        Every 5 hours: enter each screening's seat selection page
        and count how many seats are sold vs available.
        """
        all_screenings = []
        pw, browser = await self._launch_browser()
        try:
            page = await self._new_page(browser)

            for branch_id, branch_info in HOT_CINEMA_BRANCHES.items():
                _, screening_infos = await self._scrape_theater_page(page, branch_id, branch_info)

                for info in screening_infos:
                    if info["showtime"] < datetime.now():
                        continue

                    total_seats = 200
                    tickets_sold = 0

                    booking_url = info.get("booking_url", "")
                    if booking_url:
                        total, sold = await self._get_seat_count_for_screening(page, booking_url)
                        if total > 0:
                            total_seats = total
                            tickets_sold = sold
                            logger.info(
                                f"  [{info['cinema_name']}] {info['movie_title']} "
                                f"{info['showtime'].strftime('%H:%M')}: "
                                f"{tickets_sold}/{total_seats} seats sold"
                            )
                    else:
                        try:
                            await self._goto_with_stealth(
                                page,
                                f"{BASE_URL}/theater/{branch_id}/{branch_info['slug']}",
                            )
                            time_str = info["showtime"].strftime("%H:%M")
                            time_buttons = await page.query_selector_all(
                                f'text="{time_str}", [class*="time"]:has-text("{time_str}")'
                            )
                            for btn in time_buttons:
                                try:
                                    async with page.expect_navigation(timeout=10000):
                                        await btn.click()

                                    current_url = page.url
                                    if 'seats' in current_url or 'ticket' in current_url or 'booking' in current_url:
                                        await page.wait_for_timeout(3000)
                                        total, sold = await self._count_seats_on_page(page)
                                        if total > 0:
                                            total_seats = total
                                            tickets_sold = sold
                                    break
                                except Exception:
                                    continue
                        except Exception as e:
                            logger.debug(f"Click-to-booking failed for {info['movie_title']}: {e}")

                    screening = ScrapedScreening(
                        movie_title=info["movie_title"],
                        cinema_name=info["cinema_name"],
                        city=info["city"],
                        showtime=info["showtime"],
                        hall=info["hall"],
                        format=info["format"],
                        language="subtitled",
                        ticket_price=39.0,
                        total_seats=total_seats,
                        tickets_sold=tickets_sold,
                    )
                    screening.revenue = screening.tickets_sold * screening.ticket_price
                    all_screenings.append(screening)

            await page.close()
        finally:
            await browser.close()
            await pw.stop()

        logger.info(f"[Hot Cinema] Ticket update: {len(all_screenings)} screenings counted")
        return all_screenings
