"""
Scraper for Hot Cinema chain using Playwright.

Playwright is used with stealth-like settings to bypass basic bot detection.
The bundled Chromium binary is auto-detected so no external Chrome install
is needed.

URL patterns:
- Theater page:  https://hotcinema.co.il/theater/{id}/{slug}
- Movie page:    https://hotcinema.co.il/movie/{id}/{slug}
- Tickets:       https://tickets.hotcinema.co.il/...

Flow:
1. Visit theater pages → collect movie URLs (/movie/{id}/{slug})
2. Visit each movie page → parse screening table (cinema, time, format)
3. For ticket updates → click showtime → select ticket → seat map → count seats

Schedule:
- Weekly:  scrape_movies()         - full movie catalog from all theaters
- Daily:   scrape_screenings()     - screening schedule from movie pages
- 5 hours: scrape_ticket_updates() - enter each screening's seat page

Proxy:
    SCRAPER_PROXY_SERVER=http://user:pass@host:port

Debug:
    A screenshot is saved to backend/debug.png after the first page load
    so you can visually confirm whether you're getting a CAPTCHA / block.
"""
import asyncio
import logging
import os
import random
import re
from datetime import datetime, timedelta

from playwright.async_api import async_playwright, Page, Browser

try:
    from playwright_stealth import stealth_async
except ImportError:
    stealth_async = None

from scrapers.base import BaseScraper, ScrapedMovie, ScrapedScreening

logger = logging.getLogger(__name__)

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

_DEBUG_SCREENSHOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "debug.png")

_BRANCH_KEYWORDS = [
    "מודיעין", "כפר סבא", "פתח תקווה", "רחובות", "חיפה",
    "קריון", "כרמיאל", "נהריה", "אשקלון", "אשדוד",
    "modi'in", "kfar saba", "petah tikva", "rehovot",
    "haifa", "kiryon", "karmiel", "nahariya", "ashkelon", "ashdod",
]

# Map Hebrew cinema names from screening table back to branch info
_CINEMA_NAME_MAP: dict[str, dict] = {}
for _bid, _binfo in HOT_CINEMA_BRANCHES.items():
    # The screening table shows "HOT CINEMA כפר סבא" etc.
    _CINEMA_NAME_MAP[_binfo["city_he"]] = _binfo


def _resolve_cinema(cinema_text: str) -> tuple[str, str]:
    """Map cinema text from screening table (e.g. 'HOT CINEMA כפר סבא') to (name, city)."""
    for city_he, binfo in _CINEMA_NAME_MAP.items():
        if city_he in cinema_text:
            return binfo["name"], binfo["city"]
    # Fallback: use the text as-is
    clean = cinema_text.replace("HOT CINEMA", "").strip()
    return f"Hot Cinema {clean}", clean


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
    # Browser lifecycle
    # ------------------------------------------------------------------

    @staticmethod
    async def _launch_browser() -> tuple:
        pw = await async_playwright().start()

        proxy_server = os.environ.get("SCRAPER_PROXY_SERVER")
        proxy_cfg = {"server": proxy_server} if proxy_server else None
        if proxy_cfg:
            logger.info(f"[Hot Cinema] Using proxy: {proxy_server}")

        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-infobars",
            ],
            proxy=proxy_cfg,
        )

        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/141.0.0.0 Safari/537.36"
            ),
            locale="he-IL",
            timezone_id="Asia/Jerusalem",
            java_script_enabled=True,
        )

        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => false });
            Object.defineProperty(navigator, 'languages', { get: () => ['he-IL', 'he', 'en-US', 'en'] });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            window.chrome = { runtime: {} };
        """)

        if stealth_async:
            page = await context.new_page()
            await stealth_async(page)
            await page.close()
            logger.info("[Hot Cinema] playwright-stealth patches applied")

        return pw, browser, context

    # ------------------------------------------------------------------
    # Anti-detection helpers
    # ------------------------------------------------------------------

    @staticmethod
    async def _human_delay(lo: float = 0.5, hi: float = 1.5):
        await asyncio.sleep(random.uniform(lo, hi))

    @staticmethod
    async def _simulate_human(page: Page):
        try:
            scroll_y = random.randint(150, 500)
            await page.evaluate(f"window.scrollBy(0, {scroll_y})")
            await asyncio.sleep(random.uniform(0.2, 0.5))
            await page.evaluate(f"window.scrollBy(0, -{random.randint(50, scroll_y)})")
            await asyncio.sleep(random.uniform(0.1, 0.3))
        except Exception:
            pass

    async def _open_url(self, page: Page, url: str, *, take_debug_screenshot: bool = False,
                        wait_for_network: bool = False):
        await self._human_delay()
        try:
            wait_until = "networkidle" if wait_for_network else "domcontentloaded"
            await page.goto(url, wait_until=wait_until, timeout=45000)
        except Exception as e:
            logger.warning(f"[Hot Cinema] Page load timeout for {url}: {e}")

        await asyncio.sleep(2)

        if take_debug_screenshot:
            try:
                await page.screenshot(path=_DEBUG_SCREENSHOT)
                logger.info(f"[Hot Cinema] Debug screenshot saved → {_DEBUG_SCREENSHOT}")
            except Exception as e:
                logger.debug(f"Screenshot failed: {e}")

        await self._simulate_human(page)

    # ------------------------------------------------------------------
    # Seat counting (on seat map page)
    # ------------------------------------------------------------------

    async def _count_seats_on_page(self, page: Page) -> tuple[int, int]:
        total = 0
        sold = 0

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
                    classes = (await seat.get_attribute("class") or "").lower()
                    data_status = (await seat.get_attribute("data-status") or "").lower()
                    aria_disabled = (await seat.get_attribute("aria-disabled") or "").lower()
                    style = (await seat.get_attribute("style") or "").lower()

                    is_sold = any([
                        "sold" in classes,
                        "occupied" in classes,
                        "taken" in classes,
                        "unavailable" in classes,
                        "reserved" in classes,
                        "disabled" in classes,
                        "booked" in classes,
                        "תפוס" in classes,
                        data_status in ("sold", "occupied", "taken", "unavailable", "reserved", "booked"),
                        aria_disabled == "true",
                        "pointer-events: none" in style and "opacity" in style,
                    ])

                    fill = (await seat.get_attribute("fill") or "").lower()
                    if fill and not is_sold:
                        sold_colors = [
                            "#ccc", "#ddd", "#999", "#888", "#666", "gray", "grey",
                            "#ff0000", "red", "#c0c0c0", "#808080",
                        ]
                        if any(c in fill for c in sold_colors):
                            is_sold = True

                    if is_sold:
                        sold += 1

                if total > 0:
                    break
            except Exception as e:
                logger.debug(f"Seat selector '{selector}' failed: {e}")
                continue

        if total == 0:
            try:
                body_text = await page.inner_text("body")
                remaining_match = re.search(r"נותרו\s+(\d+)\s+מקומות", body_text)
                if remaining_match:
                    total = int(remaining_match.group(1))
                    sold = 0
                ratio_match = re.search(r"(\d+)\s*/\s*(\d+)", body_text)
                if ratio_match:
                    sold = int(ratio_match.group(1))
                    total = int(ratio_match.group(2))
            except Exception:
                pass

        return total, sold

    # ------------------------------------------------------------------
    # Theater page scraping - collect movies + their detail URLs
    # ------------------------------------------------------------------

    async def _scrape_theater_page(self, page: Page, branch_id: str,
                                    branch_info: dict) -> list[ScrapedMovie]:
        """Visit a theater page and collect movies with their /movie/ URLs."""
        movies: list[ScrapedMovie] = []

        url = f"{BASE_URL}/theater/{branch_id}/{branch_info['slug']}"
        try:
            await self._open_url(page, url, take_debug_screenshot=(branch_id == "1"))

            # Collect all /movie/ links on the page
            movie_links = await page.query_selector_all('a[href*="/movie/"]')
            seen_urls: set[str] = set()

            for link in movie_links:
                try:
                    href = await link.get_attribute("href") or ""
                    if "/movie/" not in href:
                        continue

                    # Normalize URL
                    if href.startswith("/"):
                        full_url = f"{BASE_URL}{href}"
                    elif href.startswith("http"):
                        full_url = href
                    else:
                        continue

                    if full_url in seen_urls:
                        continue
                    seen_urls.add(full_url)

                    # Try to get title from the link or its children
                    title = ""
                    title_el = await link.query_selector(
                        'h2, h3, h4, [class*="title"], [class*="name"], [class*="Title"], [class*="Name"]'
                    )
                    if title_el:
                        title = (await title_el.inner_text()).strip()
                    if not title:
                        title = (await link.inner_text()).strip()
                    if not title or len(title) < 2:
                        continue

                    # Skip branch elements
                    if any(kw in title.lower() for kw in _BRANCH_KEYWORDS):
                        continue

                    poster_url = ""
                    img = await link.query_selector("img")
                    if img:
                        poster_url = await img.get_attribute("src") or ""
                        if poster_url and not poster_url.startswith("http"):
                            poster_url = f"{BASE_URL}{poster_url}"

                    movies.append(ScrapedMovie(
                        title=title, title_he=title,
                        poster_url=poster_url, detail_url=full_url,
                    ))
                except Exception:
                    continue

            # Fallback: broader element search if no /movie/ links found
            if not movies:
                movie_elements = await page.query_selector_all(
                    '[class*="movie"], [class*="Movie"], [class*="film"], [class*="Film"], '
                    '[data-movie], [data-film]'
                )
                if not movie_elements:
                    movie_elements = await page.query_selector_all(
                        'article, .card, [class*="item"]'
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
                        if any(kw in title.lower() for kw in _BRANCH_KEYWORDS):
                            continue

                        # Try to find a /movie/ link inside
                        detail_url = ""
                        movie_link = await elem.query_selector('a[href*="/movie/"]')
                        if movie_link:
                            href = await movie_link.get_attribute("href") or ""
                            if href.startswith("/"):
                                detail_url = f"{BASE_URL}{href}"
                            elif href.startswith("http"):
                                detail_url = href

                        poster_url = ""
                        img = await elem.query_selector("img")
                        if img:
                            poster_url = await img.get_attribute("src") or ""
                            if poster_url and not poster_url.startswith("http"):
                                poster_url = f"{BASE_URL}{poster_url}"

                        movies.append(ScrapedMovie(
                            title=title, title_he=title,
                            poster_url=poster_url, detail_url=detail_url,
                        ))
                    except Exception:
                        continue

            logger.info(f"[Hot Cinema] Branch {branch_id}: found {len(movies)} movies")
        except Exception as e:
            logger.warning(f"[Hot Cinema] Theater {branch_id} page failed: {e}")

        return movies

    # ------------------------------------------------------------------
    # Movie page scraping - extract screening table
    # ------------------------------------------------------------------

    async def _scrape_movie_screenings(self, page: Page, movie_url: str,
                                        movie_title: str) -> list[dict]:
        """Visit a movie detail page and extract screening data.

        Uses network interception to capture the movieevents API response
        which contains complete screening data (theater, date, time, format).
        """
        api_responses: list[list] = []

        async def capture_response(response):
            url = response.url
            if "movieevents" not in url:
                return
            try:
                body = await response.json()
                if isinstance(body, list) and body:
                    api_responses.append(body)
            except Exception:
                pass

        try:
            page.on("response", capture_response)
            await self._open_url(page, movie_url, wait_for_network=True)

            # Scroll to trigger dynamic loading of screening section
            await page.evaluate("window.scrollBy(0, 800)")
            await asyncio.sleep(2)
            await page.evaluate("window.scrollBy(0, 500)")
            await asyncio.sleep(1)

        except Exception as e:
            logger.warning(f"[Hot Cinema] Movie page failed for '{movie_title}': {e}")
        finally:
            page.remove_listener("response", capture_response)

        # Parse screening data from the movieevents API response
        parsed: list[dict] = []

        if not api_responses:
            logger.info(f"[Hot Cinema] '{movie_title}': no movieevents API response captured")
            return parsed

        # Use the first captured response (duplicates are common)
        theaters = api_responses[0]
        logger.info(f"[Hot Cinema] '{movie_title}': movieevents returned {len(theaters)} theater(s)")

        for theater in theaters:
            theater_name = theater.get("TheaterName", "")
            theater_id = theater.get("TheaterID")

            # Resolve cinema name and city from theater name
            cinema_name, city = _resolve_cinema(f"HOT CINEMA {theater_name}")

            # Determine format from screening type flags
            is_3d = theater.get("Is3D")
            is_atmos_2d = theater.get("IsAtmos2D")
            is_atmos_3d = theater.get("IsAtmos3D")
            screening_type = theater.get("ScreeningType", "")

            screen_format = "2D"
            upper_type = screening_type.upper()
            if "IMAX" in upper_type:
                screen_format = "IMAX"
            elif "4DX" in upper_type:
                screen_format = "4DX"
            elif "SCREENX" in upper_type:
                screen_format = "ScreenX"
            elif is_atmos_3d:
                screen_format = "ATMOS 3D"
            elif is_atmos_2d:
                screen_format = "ATMOS"
            elif is_3d:
                screen_format = "3D"

            # Base language from theater-level fields
            dubbed_lang = theater.get("DubbedLanguage")
            subtitled_lang = theater.get("SubtitledLanguage")

            dates = theater.get("Dates", [])
            for date_entry in dates:
                try:
                    date_str = date_entry.get("FormattedDate") or date_entry.get("Date", "")
                    if not date_str:
                        continue

                    # Parse "2026-04-04 11:00:00" or "2026-04-04T11:00:00"
                    date_str = date_str.replace("T", " ")
                    if len(date_str) >= 16:
                        showtime = datetime.strptime(date_str[:19], "%Y-%m-%d %H:%M:%S")
                    else:
                        continue

                    # Per-screening language override
                    entry_dubbed = date_entry.get("DubbedLanguage") or dubbed_lang
                    entry_subtitled = date_entry.get("SubtitledLanguage") or subtitled_lang

                    if entry_dubbed:
                        language = "dubbed"
                    elif entry_subtitled:
                        language = "subtitled"
                    else:
                        language = "original"

                    # Per-screening format override
                    entry_3d = date_entry.get("Is3D")
                    entry_atmos_2d = date_entry.get("IsAtmos2D")
                    entry_atmos_3d = date_entry.get("IsAtmos3D")
                    entry_format = screen_format
                    if entry_atmos_3d:
                        entry_format = "ATMOS 3D"
                    elif entry_atmos_2d:
                        entry_format = "ATMOS"
                    elif entry_3d:
                        entry_format = "3D"

                    event_id = date_entry.get("EventId", "")

                    parsed.append({
                        "movie_title": movie_title,
                        "cinema_name": cinema_name,
                        "city": city,
                        "showtime": showtime,
                        "hall": "",
                        "format": entry_format,
                        "language": language,
                        "booking_url": f"{TICKETS_URL}/site/hotcinema/{theater_id}/{event_id}",
                    })
                except Exception as e:
                    logger.debug(f"[Hot Cinema] Failed to parse date entry for '{movie_title}': {e}")
                    continue

        logger.info(f"[Hot Cinema] '{movie_title}': {len(parsed)} screenings parsed from API")
        return parsed

    async def _scrape_movie_detail(self, page: Page, movie_path: str) -> ScrapedMovie | None:
        try:
            url = f"{BASE_URL}{movie_path}" if movie_path.startswith("/") else movie_path
            await self._open_url(page, url)

            title = ""
            title_el = await page.query_selector('h1, [class*="movieTitle"], [class*="MovieTitle"]')
            if title_el:
                title = (await title_el.inner_text()).strip()

            genre = ""
            genre_el = await page.query_selector('[class*="genre"], [class*="Genre"]')
            if genre_el:
                genre = (await genre_el.inner_text()).strip()

            duration = 0
            dur_el = await page.query_selector('[class*="duration"], [class*="Duration"], [class*="length"]')
            if dur_el:
                dur_text = (await dur_el.inner_text()).strip()
                dur_match = re.search(r"(\d+)", dur_text)
                if dur_match:
                    duration = int(dur_match.group(1))

            director = ""
            dir_el = await page.query_selector('[class*="director"], [class*="Director"]')
            if dir_el:
                director = (await dir_el.inner_text()).strip()

            poster_url = ""
            poster_el = await page.query_selector(
                '[class*="poster"] img, [class*="Poster"] img, .movie-image img'
            )
            if poster_el:
                poster_url = await poster_el.get_attribute("src") or ""
                if poster_url and not poster_url.startswith("http"):
                    poster_url = f"{BASE_URL}{poster_url}"

            if title:
                return ScrapedMovie(
                    title=title, title_he=title, genre=genre,
                    duration_minutes=duration, poster_url=poster_url,
                    director=director, detail_url=url,
                )
        except Exception as e:
            logger.warning(f"Movie detail scrape failed for {movie_path}: {e}")
        return None

    # ------------------------------------------------------------------
    # Navigate ticket purchase flow to reach seat map
    # ------------------------------------------------------------------

    async def _navigate_to_seat_map(self, page: Page, booking_url: str) -> tuple[int, int]:
        """Navigate from booking URL through ticket selection to seat map.

        Flow: booking_url → ticket page (click + for regular) → seat map page
        """
        try:
            await self._open_url(page, booking_url)
            await asyncio.sleep(2)

            # Check if we're already on a seat map page
            if "seats" in page.url or "מושבים" in (await page.inner_text("body"))[:200]:
                return await self._count_seats_on_page(page)

            # Look for the + button to add a regular ticket
            # The ticket page shows rows like: "רגיל  ₪50.50  - 1 +"
            plus_buttons = await page.query_selector_all(
                'button, [role="button"], [class*="plus"], [class*="Plus"], '
                '[class*="increase"], [class*="add"]'
            )
            clicked = False
            for btn in plus_buttons:
                try:
                    text = (await btn.inner_text()).strip()
                    if text == "+" or text == "＋":
                        await btn.click()
                        clicked = True
                        logger.debug("[Hot Cinema] Clicked + button for ticket")
                        await asyncio.sleep(1)
                        break
                except Exception:
                    continue

            if not clicked:
                # Try finding + by aria-label or nearby "רגיל" text
                try:
                    plus_btn = await page.query_selector(
                        '[aria-label*="הוסף"], [aria-label*="plus"], '
                        '[aria-label*="increase"]'
                    )
                    if plus_btn:
                        await plus_btn.click()
                        clicked = True
                        await asyncio.sleep(1)
                except Exception:
                    pass

            if not clicked:
                logger.debug("[Hot Cinema] Could not find + button, trying to proceed anyway")

            # Look for "continue" / "המשך" / "מושבים" button
            proceed_selectors = [
                'button:has-text("המשך")',
                'button:has-text("מושבים")',
                'a:has-text("המשך")',
                'a:has-text("מושבים")',
                '[class*="continue"], [class*="Continue"]',
                '[class*="next"], [class*="Next"]',
                '[class*="submit"], [class*="Submit"]',
            ]
            for sel in proceed_selectors:
                try:
                    btn = await page.query_selector(sel)
                    if btn:
                        await btn.click()
                        await asyncio.sleep(3)
                        break
                except Exception:
                    continue

            # Now we should be on the seat map page
            return await self._count_seats_on_page(page)

        except Exception as e:
            logger.debug(f"[Hot Cinema] Seat map navigation failed: {e}")
            return 0, 0

    # ------------------------------------------------------------------
    # Scrape implementations
    # ------------------------------------------------------------------

    async def scrape_movies(self) -> list[ScrapedMovie]:
        """Weekly: scrape all movies from all branches + homepage."""
        all_movies: dict[str, ScrapedMovie] = {}
        pw, browser, context = await self._launch_browser()
        try:
            page = await context.new_page()

            # Collect movies from theater pages
            for branch_id, branch_info in HOT_CINEMA_BRANCHES.items():
                movies = await self._scrape_theater_page(page, branch_id, branch_info)
                for m in movies:
                    if m.title and m.title not in all_movies:
                        all_movies[m.title] = m

            # Also collect from homepage movie detail pages
            try:
                await self._open_url(page, BASE_URL)
                links = await page.query_selector_all('a[href*="/movie/"]')
                movie_paths: set[str] = set()
                for link in links:
                    href = await link.get_attribute("href") or ""
                    if "/movie/" in href:
                        movie_paths.add(href)
                for path in list(movie_paths)[:30]:
                    movie = await self._scrape_movie_detail(page, path)
                    if movie and movie.title and movie.title not in all_movies:
                        all_movies[movie.title] = movie
            except Exception as e:
                logger.warning(f"[Hot Cinema] Homepage scrape failed: {e}")
        finally:
            await browser.close()
            await pw.stop()

        result = list(all_movies.values())
        if not result:
            logger.warning("[Hot Cinema] No movies found - site may be unreachable or structure changed")
        logger.info(f"[Hot Cinema] Scraped {len(result)} unique movies")
        return result

    async def scrape_screenings(self) -> list[ScrapedScreening]:
        """Daily: visit each movie page and parse the screening table."""
        all_screenings: list[ScrapedScreening] = []
        pw, browser, context = await self._launch_browser()
        try:
            page = await context.new_page()

            # Step 1: Collect unique movie URLs (limited to 1 branch for testing)
            movie_urls: dict[str, str] = {}  # title -> URL
            for branch_id, branch_info in list(HOT_CINEMA_BRANCHES.items())[:1]:
                movies = await self._scrape_theater_page(page, branch_id, branch_info)
                for m in movies:
                    if m.detail_url and m.title not in movie_urls:
                        movie_urls[m.title] = m.detail_url

            # Also check homepage for additional movie URLs
            try:
                await self._open_url(page, BASE_URL)
                links = await page.query_selector_all('a[href*="/movie/"]')
                for link in links:
                    href = await link.get_attribute("href") or ""
                    if "/movie/" not in href:
                        continue
                    full_url = f"{BASE_URL}{href}" if href.startswith("/") else href
                    # Try to get title
                    try:
                        text = (await link.inner_text()).strip()
                        if text and len(text) >= 2 and text not in movie_urls:
                            movie_urls[text] = full_url
                    except Exception:
                        continue
            except Exception:
                pass

            logger.info(f"[Hot Cinema] Found {len(movie_urls)} unique movie URLs to check for screenings")

            # Limit to 3 movies for testing
            _test_items = list(movie_urls.items())[:3]
            logger.info(f"[Hot Cinema] Testing with {len(_test_items)} movies")

            # Step 2: Visit each movie page to get screenings
            for title, url in _test_items:
                screening_infos = await self._scrape_movie_screenings(page, url, title)

                for info in screening_infos:
                    screening = ScrapedScreening(
                        movie_title=info["movie_title"],
                        cinema_name=info["cinema_name"],
                        city=info["city"],
                        showtime=info["showtime"],
                        hall=info["hall"],
                        format=info["format"],
                        language=info.get("language", "subtitled"),
                        ticket_price=39.0,
                        total_seats=200,
                        tickets_sold=0,
                    )
                    screening.revenue = 0
                    all_screenings.append(screening)
        finally:
            await browser.close()
            await pw.stop()

        logger.info(f"[Hot Cinema] Daily scrape: {len(all_screenings)} screenings from {len(movie_urls)} movies")
        return all_screenings

    async def scrape_ticket_updates(self) -> list[ScrapedScreening]:
        """Every 5 hours: visit movie pages, then navigate to seat maps to count seats."""
        all_screenings: list[ScrapedScreening] = []
        pw, browser, context = await self._launch_browser()
        try:
            page = await context.new_page()

            # Collect movie URLs
            movie_urls: dict[str, str] = {}
            for branch_id, branch_info in HOT_CINEMA_BRANCHES.items():
                movies = await self._scrape_theater_page(page, branch_id, branch_info)
                for m in movies:
                    if m.detail_url and m.title not in movie_urls:
                        movie_urls[m.title] = m.detail_url

            logger.info(f"[Hot Cinema] Ticket update: checking {len(movie_urls)} movies")

            for title, url in movie_urls.items():
                screening_infos = await self._scrape_movie_screenings(page, url, title)

                for info in screening_infos:
                    if info["showtime"] < datetime.now():
                        continue

                    total_seats = 200
                    tickets_sold = 0

                    booking_url = info.get("booking_url", "")
                    if booking_url:
                        total, sold = await self._navigate_to_seat_map(page, booking_url)
                        if total > 0:
                            total_seats = total
                            tickets_sold = sold
                            logger.info(
                                f"  [{info['cinema_name']}] {info['movie_title']} "
                                f"{info['showtime'].strftime('%H:%M')}: "
                                f"{tickets_sold}/{total_seats} seats sold"
                            )

                    screening = ScrapedScreening(
                        movie_title=info["movie_title"],
                        cinema_name=info["cinema_name"],
                        city=info["city"],
                        showtime=info["showtime"],
                        hall=info["hall"],
                        format=info["format"],
                        language=info.get("language", "subtitled"),
                        ticket_price=39.0,
                        total_seats=total_seats,
                        tickets_sold=tickets_sold,
                    )
                    screening.revenue = screening.tickets_sold * screening.ticket_price
                    all_screenings.append(screening)
        finally:
            await browser.close()
            await pw.stop()

        logger.info(f"[Hot Cinema] Ticket update: {len(all_screenings)} screenings counted")
        return all_screenings
