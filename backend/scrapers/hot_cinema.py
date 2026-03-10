"""
Scraper for Hot Cinema chain using Playwright.

Playwright is used with stealth-like settings to bypass basic bot detection.
The bundled Chromium binary is auto-detected so no external Chrome install
is needed.

URL patterns:
- Theater page:  https://hotcinema.co.il/theater/{id}/{slug}
- Movie page:    https://hotcinema.co.il/movie/{id}/{slug}
- Seat map:      https://tickets.hotcinema.co.il/site/{siteId}/seats

Schedule:
- Weekly:  scrape_movies()         - full movie catalog from all theaters
- Daily:   scrape_screenings()     - screening schedule for next 7 days
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

# Where to save the debug screenshot (relative to the backend dir)
_DEBUG_SCREENSHOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "debug.png")

# Branch-name keywords used to filter out non-movie elements
_BRANCH_KEYWORDS = [
    "מודיעין", "כפר סבא", "פתח תקווה", "רחובות", "חיפה",
    "קריון", "כרמיאל", "נהריה", "אשקלון", "אשדוד",
    "modi'in", "kfar saba", "petah tikva", "rehovot",
    "haifa", "kiryon", "karmiel", "nahariya", "ashkelon", "ashdod",
]


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
        """Launch Playwright Chromium with stealth-like settings.

        Returns (playwright_instance, browser, context).
        """
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
            # Stealth-ish: hide automation indicators
            java_script_enabled=True,
        )

        # Hide webdriver flag (fallback if playwright-stealth not installed)
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => false });
            // Hide automation indicators
            Object.defineProperty(navigator, 'languages', { get: () => ['he-IL', 'he', 'en-US', 'en'] });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            window.chrome = { runtime: {} };
        """)

        # Apply full stealth patches if available
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
    async def _human_delay(lo: float = 1.0, hi: float = 3.0):
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

    async def _open_url(self, page: Page, url: str, *, take_debug_screenshot: bool = False):
        await self._human_delay()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        except Exception as e:
            logger.warning(f"[Hot Cinema] Page load timeout for {url}: {e}")

        # Extra wait for JS rendering
        await asyncio.sleep(2)

        if take_debug_screenshot:
            try:
                await page.screenshot(path=_DEBUG_SCREENSHOT)
                logger.info(f"[Hot Cinema] Debug screenshot saved → {_DEBUG_SCREENSHOT}")
            except Exception as e:
                logger.debug(f"Screenshot failed: {e}")

        await self._simulate_human(page)

    # ------------------------------------------------------------------
    # Seat counting
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

    async def _get_seat_count_for_screening(self, page: Page, screening_url: str) -> tuple[int, int]:
        try:
            await self._open_url(page, screening_url)
            await asyncio.sleep(3)
            return await self._count_seats_on_page(page)
        except Exception as e:
            logger.warning(f"Seat count failed for {screening_url}: {e}")
            return 0, 0

    # ------------------------------------------------------------------
    # Theater page scraping
    # ------------------------------------------------------------------

    async def _scrape_theater_page(self, page: Page, branch_id: str,
                                    branch_info: dict) -> tuple[list[ScrapedMovie], list[dict]]:
        movies: list[ScrapedMovie] = []
        screening_infos: list[dict] = []

        url = f"{BASE_URL}/theater/{branch_id}/{branch_info['slug']}"
        try:
            await self._open_url(page, url, take_debug_screenshot=(branch_id == "1"))

            # Try specific movie selectors first
            movie_elements = await page.query_selector_all(
                '[class*="movie"], [class*="Movie"], [class*="film"], [class*="Film"], '
                '[data-movie], [data-film]'
            )
            # Fallback to broader selectors
            if not movie_elements:
                movie_elements = await page.query_selector_all(
                    'article, .card, [class*="item"]'
                )

            logger.info(f"[Hot Cinema] Branch {branch_id}: found {len(movie_elements)} candidate elements")

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

                    # Skip branch/cinema elements
                    elem_text = (await elem.inner_text()).lower()
                    elem_href = (await elem.get_attribute("href") or "").lower()
                    is_branch = any(kw in (elem_text + " " + elem_href) for kw in [
                        "/theater/", "/branch/", "/cinema/", "סניף", "סניפים",
                    ])
                    if is_branch or any(kw in title.lower() for kw in _BRANCH_KEYWORDS):
                        logger.debug(f"Skipping branch element: {title}")
                        continue

                    poster_url = ""
                    img = await elem.query_selector("img")
                    if img:
                        poster_url = await img.get_attribute("src") or ""
                        if poster_url and not poster_url.startswith("http"):
                            poster_url = f"{BASE_URL}{poster_url}"

                    movies.append(ScrapedMovie(title=title, title_he=title, poster_url=poster_url))

                    # Find showtime elements
                    showtime_els = await elem.query_selector_all(
                        'a[href*="tickets"], a[href*="booking"], a[href*="order"], '
                        'a[href*="seats"], a[href*="site"], '
                        '[class*="showtime"], [class*="time"], [class*="screening"], '
                        '[class*="Showtime"], [class*="Time"], [class*="Screening"], '
                        'button[class*="time"], a[class*="time"]'
                    )

                    for st_el in showtime_els:
                        try:
                            time_text = (await st_el.inner_text()).strip()
                            time_match = re.search(r"(\d{1,2}):(\d{2})", time_text)
                            if not time_match:
                                continue

                            hour, minute = int(time_match.group(1)), int(time_match.group(2))
                            showtime = datetime.now().replace(
                                hour=hour, minute=minute, second=0, microsecond=0,
                            )
                            if showtime < datetime.now():
                                showtime += timedelta(days=1)

                            # Detect format
                            format_text = time_text.upper()
                            parent_text = ""
                            try:
                                parent = await page.evaluate(
                                    """el => {
                                        const p = el.closest('[class*=format],[class*=Format],[class*=type],[class*=Type]');
                                        return p ? p.textContent : '';
                                    }""",
                                    st_el,
                                )
                                parent_text = (parent or "").upper()
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

                            hall = await st_el.get_attribute("data-hall") or ""

                            # Extract booking URL
                            booking_url = ""
                            href = await st_el.get_attribute("href") or ""
                            if href:
                                if href.startswith("http"):
                                    booking_url = href
                                elif href.startswith("/"):
                                    booking_url = (
                                        f"{TICKETS_URL}{href}"
                                        if "site" in href or "seats" in href
                                        else f"{BASE_URL}{href}"
                                    )
                            else:
                                data_url = (
                                    await st_el.get_attribute("data-url")
                                    or await st_el.get_attribute("data-href")
                                    or ""
                                )
                                if data_url:
                                    booking_url = (
                                        data_url if data_url.startswith("http")
                                        else f"{TICKETS_URL}{data_url}"
                                    )

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
                    duration_minutes=duration, poster_url=poster_url, director=director,
                )
        except Exception as e:
            logger.warning(f"Movie detail scrape failed for {movie_path}: {e}")
        return None

    # ------------------------------------------------------------------
    # Scrape implementations
    # ------------------------------------------------------------------

    async def scrape_movies(self) -> list[ScrapedMovie]:
        """Weekly: scrape all movies from all branches + homepage."""
        all_movies: dict[str, ScrapedMovie] = {}
        pw, browser, context = await self._launch_browser()
        try:
            page = await context.new_page()

            for branch_id, branch_info in HOT_CINEMA_BRANCHES.items():
                movies, _ = await self._scrape_theater_page(page, branch_id, branch_info)
                for m in movies:
                    if m.title and m.title not in all_movies:
                        all_movies[m.title] = m

            # Movie detail pages from homepage
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
                logger.warning(f"Hot Cinema homepage scrape failed: {e}")
        finally:
            await browser.close()
            await pw.stop()

        result = list(all_movies.values())
        if not result:
            logger.warning("[Hot Cinema] No movies found - site may be unreachable or structure changed")
        logger.info(f"[Hot Cinema] Scraped {len(result)} unique movies")
        return result

    async def scrape_screenings(self) -> list[ScrapedScreening]:
        """Daily: scrape screenings for next 7 days."""
        all_screenings: list[ScrapedScreening] = []
        pw, browser, context = await self._launch_browser()
        try:
            page = await context.new_page()

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
                            await self._human_delay(1.5, 3.0)
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
        finally:
            await browser.close()
            await pw.stop()

        logger.info(f"[Hot Cinema] Daily scrape: {len(all_screenings)} screenings")
        return all_screenings

    async def scrape_ticket_updates(self) -> list[ScrapedScreening]:
        """Every 5 hours: count seats sold for active screenings."""
        all_screenings: list[ScrapedScreening] = []
        pw, browser, context = await self._launch_browser()
        try:
            page = await context.new_page()

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
                            await self._open_url(
                                page,
                                f"{BASE_URL}/theater/{branch_id}/{branch_info['slug']}",
                            )
                            time_str = info["showtime"].strftime("%H:%M")
                            time_buttons = await page.query_selector_all('[class*="time"]')
                            for btn in time_buttons:
                                btn_text = (await btn.inner_text()).strip()
                                if time_str not in btn_text:
                                    continue
                                try:
                                    await btn.click()
                                    await asyncio.sleep(3)
                                    current_url = page.url
                                    if any(kw in current_url for kw in ("seats", "ticket", "booking")):
                                        await asyncio.sleep(3)
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
        finally:
            await browser.close()
            await pw.stop()

        logger.info(f"[Hot Cinema] Ticket update: {len(all_screenings)} screenings counted")
        return all_screenings
