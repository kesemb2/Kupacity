"""
Scraper for Movieland cinema chain using Playwright.

Movieland uses the BiggerPicture (ecom.biggerpicture.ai) platform for ticket
booking.  Clicking a showtime on a branch page opens the seat-map page
directly — no intermediate quantity / continue steps.

Navigation flow:
1. Open movieland.co.il
2. Hover over "סניפים" in header → dropdown with branch links
3. Visit each branch page → shows movies with showtimes for that branch
4. Each showtime link → ecom.biggerpicture.ai seat map directly

URL patterns:
- Main site:     https://movieland.co.il/
- Branch page:   https://movieland.co.il/{branch-slug}/  (e.g. /tel-aviv/)
- Seat map:      https://ecom.biggerpicture.ai/...  (random ID per screening)

Seat image patterns (all under /mvl-seat/):
  /available/seat.png              regular seat — available
  /unavailable/seat.png            regular seat — sold
  /available/seat-ac.png           armchair — available
  /unavailable/seat-ac.png         armchair — sold
  /available/love-seat-left-sl.png   love seat L — available
  /unavailable/love-seat-left-sl.png love seat L — sold
  /available/love-seat-right-sr.png  love seat R — available
  /unavailable/love-seat-right-sr.png love seat R — sold
  /available/love-seat-left-lsl.png  long love L — available
  /unavailable/love-seat-left-lsl.png long love L — sold
  /available/love-seat-right-lsr.png long love R — available
  /unavailable/love-seat-right-lsr.png long love R — sold
  /available/handicap-seat.png     handicap — available
  /unavailable/handicap-seat.png   handicap — sold

Schedule:
- Weekly:  scrape_movies()         — full movie catalog
- Daily:   scrape_screenings()     — screening schedule (next 7 days)
- 5 hours: scrape_ticket_updates() — seat-map counts
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

# ── Branch data ──────────────────────────────────────────────────────────────

MOVIELAND_BRANCHES = {
    "tel_aviv": {
        "name": "Movieland Tel Aviv",
        "name_he": "מובילנד תל אביב",
        "city": "Tel Aviv",
        "city_he": "תל אביב",
        "slug": "tel-aviv",
    },
    "netanya": {
        "name": "Movieland Netanya",
        "name_he": "מובילנד נתניה",
        "city": "Netanya",
        "city_he": "נתניה",
        "slug": "netanya",
    },
    "haifa": {
        "name": "Movieland Haifa",
        "name_he": "מובילנד חיפה",
        "city": "Haifa",
        "city_he": "חיפה",
        "slug": "haifa",
    },
    "karmiel": {
        "name": "Movieland Karmiel",
        "name_he": "מובילנד כרמיאל",
        "city": "Karmiel",
        "city_he": "כרמיאל",
        "slug": "karmiel",
    },
    "afula": {
        "name": "Movieland Afula",
        "name_he": "מובילנד עפולה",
        "city": "Afula",
        "city_he": "עפולה",
        "slug": "afula",
    },
}

BASE_URL = "https://movieland.co.il"
BOOKING_DOMAIN = "ecom.biggerpicture.ai"

_DEBUG_SCREENSHOTS_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "debug_screenshots"
)

# Map Hebrew branch names (as seen on the site) to branch info
_BRANCH_NAME_MAP: dict[str, dict] = {}
for _bid, _binfo in MOVIELAND_BRANCHES.items():
    _BRANCH_NAME_MAP[_binfo["city_he"]] = _binfo
    _BRANCH_NAME_MAP[_binfo["name_he"]] = _binfo


def _resolve_cinema(text: str) -> tuple[str, str]:
    """Map cinema text to (english_name, english_city)."""
    for key, binfo in _BRANCH_NAME_MAP.items():
        if key in text:
            return binfo["name"], binfo["city"]
    clean = text.strip()
    return f"Movieland {clean}", clean


def _debug_screenshot_path(step: str, detail: str = "",
                           branch: str = "", movie: str = "",
                           time_str: str = "") -> str:
    os.makedirs(_DEBUG_SCREENSHOTS_DIR, exist_ok=True)
    ts = datetime.now().strftime("%H%M%S")
    safe = lambda s: re.sub(r'[^\w\u0590-\u05FF -]', '', s)[:20].strip().replace(' ', '_')
    parts = ["mvl"]
    if branch:
        parts.append(safe(branch))
    if movie:
        parts.append(safe(movie))
    if time_str:
        parts.append(time_str.replace(':', ''))
    parts.append(step)
    parts.append(ts)
    return os.path.join(_DEBUG_SCREENSHOTS_DIR, "_".join(p for p in parts if p) + ".png")


# ── Scraper class ────────────────────────────────────────────────────────────

class MovielandScraper(BaseScraper):

    @property
    def chain_name(self) -> str:
        return "Movieland"

    @property
    def chain_name_he(self) -> str:
        return "מובילנד"

    @property
    def base_url(self) -> str:
        return BASE_URL

    # ── Browser lifecycle ────────────────────────────────────────────────

    @staticmethod
    async def _launch_browser() -> tuple:
        pw = await async_playwright().start()

        proxy_server = os.environ.get("SCRAPER_PROXY_SERVER")
        proxy_cfg = {"server": proxy_server} if proxy_server else None
        if proxy_cfg:
            logger.info(f"[Movieland] Using proxy: {proxy_server}")

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
            logger.info("[Movieland] playwright-stealth patches applied")

        return pw, browser, context

    # ── Helpers ──────────────────────────────────────────────────────────

    @staticmethod
    async def _human_delay(lo: float = 0.5, hi: float = 1.5):
        await asyncio.sleep(random.uniform(lo, hi))

    async def _open_url(self, page: Page, url: str, *,
                        wait_for_network: bool = False,
                        timeout: int = 45000):
        await self._human_delay()
        try:
            wait_until = "networkidle" if wait_for_network else "domcontentloaded"
            await page.goto(url, wait_until=wait_until, timeout=timeout)
        except Exception as e:
            logger.warning(f"[Movieland] Page load timeout for {url}: {e}")

        await asyncio.sleep(2)

    # ── Branch discovery ─────────────────────────────────────────────────

    async def _discover_branch_urls(self, page: Page) -> dict[str, str]:
        """Discover branch page URLs from the site header dropdown.

        Returns {branch_key: full_url} for each known branch.
        """
        branch_urls: dict[str, str] = {}

        # Navigate to homepage first
        await self._open_url(page, BASE_URL, wait_for_network=True)

        try:
            await page.screenshot(
                path=_debug_screenshot_path("page", branch="home")
            )
        except Exception:
            pass

        # Strategy 1: Hover over "סניפים" to reveal dropdown
        try:
            # Look for navigation items containing "סניפים" (branches)
            nav_items = await page.query_selector_all(
                'nav a, header a, [class*="menu"] a, [class*="nav"] a, '
                'li a, .menu-item a'
            )
            snifim_el = None
            for item in nav_items:
                text = (await item.inner_text()).strip()
                if "סניפים" in text or "סניף" in text:
                    snifim_el = item
                    break

            if snifim_el:
                await snifim_el.hover()
                await asyncio.sleep(1.5)
                logger.info("[Movieland] Hovered on 'סניפים' menu item")

                try:
                    await page.screenshot(
                        path=_debug_screenshot_path("dropdown", branch="snifim")
                    )
                except Exception:
                    pass

                # Now look for branch links in the dropdown
                all_links = await page.query_selector_all('a[href]')
                for link in all_links:
                    try:
                        href = await link.get_attribute("href") or ""
                        text = (await link.inner_text()).strip()

                        for bid, binfo in MOVIELAND_BRANCHES.items():
                            slug = binfo["slug"]
                            # Check if link goes to this branch's page
                            if (slug in href.lower() or
                                    binfo["city_he"] in text or
                                    binfo["name_he"] in text):
                                if href.startswith("/"):
                                    href = f"{BASE_URL}{href}"
                                if href.startswith("http"):
                                    branch_urls[bid] = href
                                    logger.info(f"[Movieland] Found branch URL: {binfo['name']} -> {href}")
                    except Exception:
                        continue
        except Exception as e:
            logger.warning(f"[Movieland] Dropdown discovery failed: {e}")

        # Strategy 2: Try known URL patterns for missing branches
        for bid, binfo in MOVIELAND_BRANCHES.items():
            if bid in branch_urls:
                continue
            # Try common patterns
            for pattern in [
                f"{BASE_URL}/{binfo['slug']}/",
                f"{BASE_URL}/branch/{binfo['slug']}/",
                f"{BASE_URL}/snif/{binfo['slug']}/",
                f"{BASE_URL}/{binfo['slug']}",
            ]:
                try:
                    resp = await page.goto(pattern, wait_until="domcontentloaded", timeout=10000)
                    if resp and resp.ok:
                        # Check it's actually a branch page (has movie/showtime content)
                        content = await page.content()
                        if any(kw in content for kw in [
                            "showtime", "הקרנ", "שעת", BOOKING_DOMAIN,
                            "movie", "סרט", binfo["city_he"],
                        ]):
                            branch_urls[bid] = pattern.rstrip("/")
                            logger.info(f"[Movieland] Found branch URL by pattern: {binfo['name']} -> {pattern}")
                            break
                except Exception:
                    continue

        # Strategy 3: Search page content for branch page links
        if len(branch_urls) < len(MOVIELAND_BRANCHES):
            await self._open_url(page, BASE_URL, wait_for_network=True)
            all_links = await page.query_selector_all('a[href]')
            for link in all_links:
                try:
                    href = await link.get_attribute("href") or ""
                    text = (await link.inner_text()).strip()
                    for bid, binfo in MOVIELAND_BRANCHES.items():
                        if bid in branch_urls:
                            continue
                        if binfo["city_he"] in text or binfo["slug"] in href.lower():
                            if href.startswith("/"):
                                href = f"{BASE_URL}{href}"
                            if href.startswith("http"):
                                branch_urls[bid] = href
                except Exception:
                    continue

        logger.info(f"[Movieland] Discovered {len(branch_urls)}/{len(MOVIELAND_BRANCHES)} branch URLs")
        return branch_urls

    # ── Parse movies & showtimes from a branch page ──────────────────────

    async def _scrape_branch_page(self, page: Page, branch_url: str,
                                   binfo: dict,
                                   collect_booking_urls: bool = False,
                                   ) -> tuple[list[ScrapedMovie], list[ScrapedScreening], list[dict]]:
        """Scrape movies and showtimes from a single branch page.

        Returns (movies, screenings, booking_items).
        booking_items is populated only when collect_booking_urls=True.
        """
        movies: list[ScrapedMovie] = []
        screenings: list[ScrapedScreening] = []
        booking_items: list[dict] = []
        seen_titles: set[str] = set()

        await self._open_url(page, branch_url, wait_for_network=True)
        await asyncio.sleep(2)

        try:
            await page.screenshot(
                path=_debug_screenshot_path("branch", branch=binfo["city_he"])
            )
        except Exception:
            pass

        # Scroll to load lazy content
        for _ in range(3):
            await page.evaluate("window.scrollBy(0, 600)")
            await asyncio.sleep(0.8)
        # Scroll back to top
        await page.evaluate("window.scrollTo(0, 0)")
        await asyncio.sleep(0.5)

        # ── Collect movies ────────────────────────────────────────────
        # Look for movie sections on the branch page
        movie_sections = await page.query_selector_all(
            '[class*="movie"], [class*="Movie"], [class*="film"], [class*="Film"], '
            '[class*="event"], [class*="Event"], '
            'article, .movie-item, .film-item, .event-item'
        )

        if not movie_sections:
            # Fallback: look for headings that might be movie titles
            movie_sections = await page.query_selector_all(
                'h2, h3, h4'
            )

        # Also get all links to find booking URLs
        all_page_links = await page.query_selector_all('a[href]')
        page_html = await page.content()

        # Try to find movie cards with showtimes
        # Each movie section may contain: title, poster, showtime buttons/links
        for section in movie_sections:
            try:
                section_text = (await section.inner_text()).strip()
                if not section_text or len(section_text) < 3:
                    continue

                # Find title - first heading or prominent text
                title = ""
                title_el = await section.query_selector(
                    'h2, h3, h4, [class*="title"], [class*="Title"], '
                    '[class*="name"], [class*="Name"]'
                )
                if title_el:
                    title = (await title_el.inner_text()).strip()

                if not title:
                    # Use section's own tag if it's a heading
                    tag = await section.evaluate("el => el.tagName")
                    if tag in ("H2", "H3", "H4"):
                        title = section_text.split('\n')[0].strip()

                if not title or len(title) < 2 or title in seen_titles:
                    continue

                # Skip navigation/branch labels
                if any(binfo2["city_he"] in title for binfo2 in MOVIELAND_BRANCHES.values()):
                    continue
                skip_words = ["סניפים", "תקנון", "צור קשר", "אודות", "FAQ", "שאלות"]
                if any(w in title for w in skip_words):
                    continue

                seen_titles.add(title)

                # Get poster
                poster_url = ""
                img = await section.query_selector("img")
                if img:
                    poster_url = (await img.get_attribute("src")
                                  or await img.get_attribute("data-src")
                                  or "")
                    if poster_url and not poster_url.startswith("http"):
                        poster_url = f"{BASE_URL}{poster_url}"

                movies.append(ScrapedMovie(
                    title=title, title_he=title,
                    poster_url=poster_url,
                    detail_url=branch_url,
                ))

                # ── Collect showtimes for this movie ─────────────────
                # Look for time patterns and booking links within/near the section
                section_links = await section.query_selector_all('a[href]')
                showtime_buttons = await section.query_selector_all(
                    'button, [class*="time"], [class*="hour"], [class*="show"]'
                )

                found_times: set[str] = set()

                for link in section_links:
                    try:
                        href = await link.get_attribute("href") or ""
                        link_text = (await link.inner_text()).strip()

                        # Extract time
                        time_match = re.search(r'(\d{1,2}:\d{2})', link_text)
                        if not time_match:
                            continue

                        time_str = time_match.group(1)
                        if time_str in found_times:
                            continue
                        found_times.add(time_str)

                        # Parse datetime
                        try:
                            today = datetime.now().date()
                            h, m = map(int, time_str.split(":"))
                            showtime = datetime.combine(
                                today, datetime.min.time().replace(hour=h, minute=m)
                            )
                        except Exception:
                            continue

                        # Extract hall
                        hall = ""
                        parent_text = section_text
                        hall_match = re.search(r'אולם\s*(\d+|[A-Za-z]+)', parent_text)
                        if hall_match:
                            hall = hall_match.group(1)

                        # Extract format
                        fmt = "2D"
                        upper_text = parent_text.upper()
                        if "IMAX" in upper_text:
                            fmt = "IMAX"
                        elif "4DX" in upper_text:
                            fmt = "4DX"
                        elif "3D" in upper_text:
                            fmt = "3D"
                        elif "VIP" in upper_text:
                            fmt = "VIP"

                        screenings.append(ScrapedScreening(
                            movie_title=title,
                            cinema_name=binfo["name"],
                            city=binfo["city"],
                            showtime=showtime,
                            hall=hall,
                            format=fmt,
                        ))

                        # Collect booking URL for ticket updates
                        if collect_booking_urls and BOOKING_DOMAIN in href:
                            booking_items.append({
                                "movie_title": title,
                                "booking_url": href,
                                "cinema_name": binfo["name"],
                                "city": binfo["city"],
                                "showtime": showtime,
                                "hall": hall,
                                "format": fmt,
                                "branch_he": binfo["city_he"],
                                "time_str": time_str,
                            })

                    except Exception:
                        continue

                # Also check showtime buttons without links
                for btn in showtime_buttons:
                    try:
                        btn_text = (await btn.inner_text()).strip()
                        time_match = re.search(r'(\d{1,2}:\d{2})', btn_text)
                        if not time_match:
                            continue
                        time_str = time_match.group(1)
                        if time_str in found_times:
                            continue
                        found_times.add(time_str)

                        try:
                            today = datetime.now().date()
                            h, m = map(int, time_str.split(":"))
                            showtime = datetime.combine(
                                today, datetime.min.time().replace(hour=h, minute=m)
                            )
                        except Exception:
                            continue

                        screenings.append(ScrapedScreening(
                            movie_title=title,
                            cinema_name=binfo["name"],
                            city=binfo["city"],
                            showtime=showtime,
                        ))
                    except Exception:
                        continue

            except Exception as e:
                logger.debug(f"[Movieland] Section parse error: {e}")
                continue

        # ── Fallback: scan ALL links on the page for booking URLs ────
        if collect_booking_urls and not booking_items:
            for link in all_page_links:
                try:
                    href = await link.get_attribute("href") or ""
                    if BOOKING_DOMAIN not in href:
                        continue

                    link_text = (await link.inner_text()).strip()
                    time_match = re.search(r'(\d{1,2}:\d{2})', link_text)
                    time_str = time_match.group(1) if time_match else ""

                    # Try to find nearby movie title
                    movie_title = ""
                    try:
                        # Walk up to find movie context
                        parent_text = await link.evaluate("""el => {
                            let p = el.parentElement;
                            for (let i = 0; i < 5 && p; i++) {
                                const h = p.querySelector('h2, h3, h4, [class*="title"]');
                                if (h) return h.textContent.trim();
                                p = p.parentElement;
                            }
                            return '';
                        }""")
                        movie_title = parent_text
                    except Exception:
                        pass

                    showtime = None
                    if time_str:
                        try:
                            today = datetime.now().date()
                            h, m = map(int, time_str.split(":"))
                            showtime = datetime.combine(
                                today, datetime.min.time().replace(hour=h, minute=m)
                            )
                        except Exception:
                            pass

                    booking_items.append({
                        "movie_title": movie_title or "Unknown",
                        "booking_url": href,
                        "cinema_name": binfo["name"],
                        "city": binfo["city"],
                        "showtime": showtime,
                        "hall": "",
                        "format": "2D",
                        "branch_he": binfo["city_he"],
                        "time_str": time_str,
                    })

                except Exception:
                    continue

        # ── Also try to find booking links in page (may be outside sections)
        if not screenings:
            for link in all_page_links:
                try:
                    href = await link.get_attribute("href") or ""
                    if BOOKING_DOMAIN not in href:
                        continue
                    link_text = (await link.inner_text()).strip()
                    time_match = re.search(r'(\d{1,2}:\d{2})', link_text)
                    if not time_match:
                        continue
                    time_str = time_match.group(1)

                    try:
                        today = datetime.now().date()
                        h, m = map(int, time_str.split(":"))
                        showtime = datetime.combine(
                            today, datetime.min.time().replace(hour=h, minute=m)
                        )
                    except Exception:
                        continue

                    # Try to get movie title from context
                    movie_title = await link.evaluate("""el => {
                        let p = el.parentElement;
                        for (let i = 0; i < 5 && p; i++) {
                            const h = p.querySelector('h2, h3, h4, [class*="title"]');
                            if (h) return h.textContent.trim();
                            p = p.parentElement;
                        }
                        return '';
                    }""")

                    if movie_title and movie_title not in seen_titles:
                        seen_titles.add(movie_title)
                        movies.append(ScrapedMovie(
                            title=movie_title, title_he=movie_title,
                            detail_url=branch_url,
                        ))

                    screenings.append(ScrapedScreening(
                        movie_title=movie_title or "Unknown",
                        cinema_name=binfo["name"],
                        city=binfo["city"],
                        showtime=showtime,
                    ))

                    if collect_booking_urls:
                        booking_items.append({
                            "movie_title": movie_title or "Unknown",
                            "booking_url": href,
                            "cinema_name": binfo["name"],
                            "city": binfo["city"],
                            "showtime": showtime,
                            "hall": "",
                            "format": "2D",
                            "branch_he": binfo["city_he"],
                            "time_str": time_str,
                        })

                except Exception:
                    continue

        logger.info(
            f"[Movieland] Branch '{binfo['name']}': "
            f"{len(movies)} movies, {len(screenings)} screenings"
            + (f", {len(booking_items)} booking URLs" if collect_booking_urls else "")
        )

        return movies, screenings, booking_items

    # ── Navigate calendar days on a branch page ──────────────────────────

    async def _navigate_branch_calendar(self, page: Page, branch_url: str,
                                         binfo: dict, days: int = 7,
                                         collect_booking_urls: bool = False,
                                         ) -> tuple[list[ScrapedMovie], list[ScrapedScreening], list[dict]]:
        """Scrape a branch page across multiple days using calendar navigation."""
        all_movies: list[ScrapedMovie] = []
        all_screenings: list[ScrapedScreening] = []
        all_booking_items: list[dict] = []
        seen_titles: set[str] = set()

        # First, scrape the current day (default view)
        movies, screenings, bookings = await self._scrape_branch_page(
            page, branch_url, binfo, collect_booking_urls
        )
        all_movies.extend(movies)
        all_screenings.extend(screenings)
        all_booking_items.extend(bookings)
        for m in movies:
            seen_titles.add(m.title)

        # Try to navigate to subsequent days via calendar/date selectors
        for day_offset in range(1, days):
            target_date = datetime.now().date() + timedelta(days=day_offset)

            try:
                # Look for date/calendar navigation elements
                date_buttons = await page.query_selector_all(
                    '[class*="date"], [class*="day"], [class*="calendar"], '
                    '[class*="Date"], [class*="Day"], [class*="Calendar"], '
                    'button[data-date], a[data-date]'
                )

                clicked = False
                target_str = target_date.strftime("%d/%m")
                target_str2 = target_date.strftime("%d.%m")
                target_day = str(target_date.day)

                for btn in date_buttons:
                    try:
                        btn_text = (await btn.inner_text()).strip()
                        data_date = await btn.get_attribute("data-date") or ""

                        if (target_str in btn_text or target_str2 in btn_text or
                                target_date.isoformat() in data_date or
                                (btn_text.isdigit() and btn_text == target_day)):
                            await btn.click()
                            await asyncio.sleep(2)
                            clicked = True
                            logger.info(f"[Movieland] Clicked calendar day {target_date}")
                            break
                    except Exception:
                        continue

                if not clicked:
                    # Try clicking "next day" arrow/button
                    next_btns = await page.query_selector_all(
                        '[class*="next"], [class*="arrow"], [class*="forward"], '
                        'button[aria-label*="next"], button[aria-label*="הבא"]'
                    )
                    for btn in next_btns:
                        try:
                            await btn.click()
                            await asyncio.sleep(2)
                            clicked = True
                            break
                        except Exception:
                            continue

                if not clicked:
                    break  # No more calendar navigation possible

                # Scrape this day's content
                day_movies, day_screenings, day_bookings = await self._scrape_branch_page(
                    page, page.url, binfo, collect_booking_urls
                )

                # Adjust showtimes to the target date
                for scr in day_screenings:
                    if scr.showtime:
                        scr.showtime = scr.showtime.replace(
                            year=target_date.year,
                            month=target_date.month,
                            day=target_date.day,
                        )
                for bi in day_bookings:
                    if bi.get("showtime"):
                        bi["showtime"] = bi["showtime"].replace(
                            year=target_date.year,
                            month=target_date.month,
                            day=target_date.day,
                        )

                # Add new movies (skip duplicates)
                for m in day_movies:
                    if m.title not in seen_titles:
                        seen_titles.add(m.title)
                        all_movies.append(m)

                all_screenings.extend(day_screenings)
                all_booking_items.extend(day_bookings)

            except Exception as e:
                logger.debug(f"[Movieland] Calendar navigation failed for day {day_offset}: {e}")
                break

        return all_movies, all_screenings, all_booking_items

    # ── Seat counting (BiggerPicture seat map) ───────────────────────────

    async def _count_seats_on_page(self, page: Page,
                                    movie_title: str = "",
                                    screening_time: str = "",
                                    branch_he: str = "") -> tuple[int, int, list]:
        """Count seats on a BiggerPicture seat-map page.

        Primary detection: image href matching /mvl-seat/(available|unavailable)/
        Secondary detection: class/attribute-based (same as Hot Cinema fallback)
        """
        total = 0
        sold = 0
        sold_positions = []

        await asyncio.sleep(3)  # wait for Angular/SPA to render seat map

        seat_data = await page.evaluate("""() => {
            // ===== METHOD A: IMAGE-BASED DETECTION (Movieland specific) =====
            const imgTotal = { count: 0 };
            const imgSold = { count: 0 };
            const imgSoldPositions = [];
            const imgSamples = [];
            const seen = new Set();

            const images = document.querySelectorAll('image, [style*="mvl-seat"]');
            for (const el of images) {
                const href = el.getAttribute('href')
                    || el.getAttributeNS('http://www.w3.org/1999/xlink', 'href')
                    || '';
                const style = window.getComputedStyle(el);
                const bgImg = style.backgroundImage || '';
                const src = href + ' ' + bgImg;

                if (!src.includes('mvl-seat')) continue;

                const bbox = el.getBoundingClientRect();
                if (bbox.width < 5 || bbox.height < 5) continue;

                const posKey = Math.round(bbox.left / 5) + ',' + Math.round(bbox.top / 5);
                if (seen.has(posKey)) continue;
                seen.add(posKey);

                imgTotal.count++;

                const isSold = src.includes('/unavailable/');
                const isAvail = src.includes('/available/');

                if (isSold) {
                    imgSold.count++;
                    imgSoldPositions.push([Math.round(bbox.left / 5), Math.round(bbox.top / 5)]);
                }

                if (imgSamples.length < 15) {
                    imgSamples.push({
                        tag: el.tagName,
                        href: href.substring(0, 80),
                        bgImg: bgImg.substring(0, 80),
                        w: Math.round(bbox.width),
                        h: Math.round(bbox.height),
                        x: Math.round(bbox.left),
                        y: Math.round(bbox.top),
                        status: isSold ? 'sold' : (isAvail ? 'available' : 'unknown'),
                    });
                }
            }

            // Also check all elements for background-image containing mvl-seat
            const allEls = document.querySelectorAll('*');
            for (const el of allEls) {
                if (el.tagName === 'image' || el.tagName === 'IMAGE') continue;
                const style = window.getComputedStyle(el);
                const bgImg = style.backgroundImage || '';
                if (!bgImg.includes('mvl-seat')) continue;

                const bbox = el.getBoundingClientRect();
                if (bbox.width < 5 || bbox.height < 5) continue;

                const posKey = Math.round(bbox.left / 5) + ',' + Math.round(bbox.top / 5);
                if (seen.has(posKey)) continue;
                seen.add(posKey);

                imgTotal.count++;

                const isSold = bgImg.includes('/unavailable/');
                if (isSold) {
                    imgSold.count++;
                    imgSoldPositions.push([Math.round(bbox.left / 5), Math.round(bbox.top / 5)]);
                }

                if (imgSamples.length < 15) {
                    imgSamples.push({
                        tag: el.tagName,
                        href: '',
                        bgImg: bgImg.substring(0, 80),
                        w: Math.round(bbox.width),
                        h: Math.round(bbox.height),
                        x: Math.round(bbox.left),
                        y: Math.round(bbox.top),
                        status: isSold ? 'sold' : 'available',
                    });
                }
            }

            // ===== METHOD B: CLASS/ATTRIBUTE-BASED (generic fallback) =====
            let classTotal = 0, classSold = 0;
            const classSoldPositions = [];
            const classSamples = [];

            for (const el of allEls) {
                const cls = (typeof el.className === 'string' ? el.className
                    : el.className && el.className.baseVal ? el.className.baseVal
                    : el.getAttribute('class') || '').toLowerCase();
                const id = (el.id || '').toLowerCase();

                const isSeatElement = cls.includes('seat') || id.includes('seat');
                if (!isSeatElement) continue;

                const isAvailable = cls.includes('available') || cls.includes('free') || cls.includes('open')
                    || el.getAttribute('data-status') === 'available'
                    || el.getAttribute('data-available') === 'true';

                const isSold = cls.includes('sold') || cls.includes('occupied') || cls.includes('taken')
                    || cls.includes('reserved') || cls.includes('booked') || cls.includes('unavailable')
                    || cls.includes('disabled')
                    || el.getAttribute('data-status') === 'sold'
                    || el.getAttribute('data-status') === 'occupied'
                    || el.getAttribute('data-available') === 'false';

                if (!isAvailable && !isSold) continue;

                classTotal++;
                if (isSold) {
                    classSold++;
                    const bbox = el.getBoundingClientRect();
                    classSoldPositions.push([Math.round(bbox.left / 5), Math.round(bbox.top / 5)]);
                }

                if (classSamples.length < 10) {
                    const bbox = el.getBoundingClientRect();
                    classSamples.push({
                        tag: el.tagName, cls: cls.substring(0, 60),
                        w: Math.round(bbox.width), h: Math.round(bbox.height),
                        x: Math.round(bbox.left), y: Math.round(bbox.top),
                        status: isSold ? 'sold' : 'available',
                    });
                }
            }

            // Try to extract hall info from the page
            const hallInfo = { hall: '' };
            const pageText = document.body ? document.body.textContent : '';
            const hallMatch = pageText.match(/אולם\s*(\d+|[A-Za-z]+)/);
            if (hallMatch) hallInfo.hall = hallMatch[1];
            // Also check for "Hall X" pattern
            const hallMatch2 = pageText.match(/Hall\s*(\d+|[A-Za-z]+)/i);
            if (!hallInfo.hall && hallMatch2) hallInfo.hall = hallMatch2[1];

            const bodyHTML = document.body ? document.body.innerHTML.substring(0, 3000) : '';

            return {
                imageMethod: {
                    total: imgTotal.count,
                    sold: imgSold.count,
                    soldPositions: imgSoldPositions,
                    samples: imgSamples,
                },
                classMethod: {
                    total: classTotal,
                    sold: classSold,
                    soldPositions: classSoldPositions,
                    samples: classSamples,
                },
                hallInfo,
                bodyHTML,
            };
        }""")

        # Take debug screenshot
        try:
            await page.screenshot(path=_debug_screenshot_path(
                "seats", movie=movie_title, branch=branch_he,
                time_str=screening_time,
            ))
        except Exception:
            pass

        hall_from_page = ""

        if seat_data:
            img = seat_data.get("imageMethod", {})
            cls = seat_data.get("classMethod", {})
            hall_from_page = seat_data.get("hallInfo", {}).get("hall", "")

            logger.info(
                f"[Movieland] Seats IMAGE method: {img.get('total', 0)} total, "
                f"{img.get('sold', 0)} sold | CLASS: {cls.get('total', 0)} total, "
                f"{cls.get('sold', 0)} sold"
                + (f" | Hall: {hall_from_page}" if hall_from_page else "")
            )

            img_total = img.get("total", 0)
            class_total = cls.get("total", 0)

            if img_total >= 10 or class_total >= 10:
                if img_total >= class_total:
                    total = img_total
                    sold = img.get("sold", 0)
                    sold_positions = img.get("soldPositions", [])
                    logger.info(f"[Movieland] Using IMAGE method: {sold}/{total}")
                else:
                    total = class_total
                    sold = cls.get("sold", 0)
                    sold_positions = cls.get("soldPositions", [])
                    logger.info(f"[Movieland] Using CLASS method: {sold}/{total}")
            else:
                logger.warning(
                    f"[Movieland] Both methods found too few seats. "
                    f"image={img_total}, class={class_total}"
                )
                html = seat_data.get("bodyHTML", "")
                if html:
                    logger.warning(f"[Movieland] Page HTML (first 500): {html[:500]}")

        return total, sold, sold_positions

    # ── Movie discovery ──────────────────────────────────────────────────

    async def scrape_movies(self, on_progress=None) -> list[ScrapedMovie]:
        """Scrape movie catalog by visiting each branch page."""
        pw, browser, context = await self._launch_browser()
        all_movies: list[ScrapedMovie] = []
        seen_titles: set[str] = set()

        try:
            page = await context.new_page()

            if on_progress:
                on_progress("סריקת סרטים - מובילנד", 0, len(MOVIELAND_BRANCHES), "מחפש סניפים")

            branch_urls = await self._discover_branch_urls(page)

            for idx, (bid, binfo) in enumerate(MOVIELAND_BRANCHES.items()):
                if on_progress:
                    on_progress("סריקת סרטים - מובילנד", idx + 1,
                                len(MOVIELAND_BRANCHES), binfo["name_he"])

                url = branch_urls.get(bid)
                if not url:
                    logger.warning(f"[Movieland] No URL for branch {binfo['name']}, skipping")
                    continue

                try:
                    movies, _, _ = await self._scrape_branch_page(page, url, binfo)

                    for m in movies:
                        if m.title not in seen_titles:
                            seen_titles.add(m.title)
                            all_movies.append(m)
                except Exception as e:
                    logger.warning(f"[Movieland] Branch {binfo['name']} failed: {e}")
                    continue

                await self._human_delay(0.5, 1.0)

            if on_progress:
                on_progress("סריקת סרטים - מובילנד", len(MOVIELAND_BRANCHES),
                            len(MOVIELAND_BRANCHES), f"נמצאו {len(all_movies)} סרטים")

            logger.info(f"[Movieland] Total movies found: {len(all_movies)}")

        except Exception as e:
            logger.error(f"[Movieland] scrape_movies failed: {e}")
        finally:
            await browser.close()
            await pw.stop()

        return all_movies

    # ── Screening discovery ──────────────────────────────────────────────

    async def scrape_screenings(self, on_progress=None) -> list[ScrapedScreening]:
        """Scrape screening schedule from all branch pages with calendar navigation."""
        pw, browser, context = await self._launch_browser()
        all_screenings: list[ScrapedScreening] = []

        try:
            page = await context.new_page()

            if on_progress:
                on_progress("סריקת הקרנות - מובילנד", 0, len(MOVIELAND_BRANCHES), "מחפש סניפים")

            branch_urls = await self._discover_branch_urls(page)

            for idx, (bid, binfo) in enumerate(MOVIELAND_BRANCHES.items()):
                if on_progress:
                    on_progress("סריקת הקרנות - מובילנד", idx + 1,
                                len(MOVIELAND_BRANCHES), binfo["name_he"])

                url = branch_urls.get(bid)
                if not url:
                    logger.warning(f"[Movieland] No URL for branch {binfo['name']}, skipping")
                    continue

                try:
                    _, screenings, _ = await self._navigate_branch_calendar(
                        page, url, binfo, days=7
                    )
                    all_screenings.extend(screenings)
                except Exception as e:
                    logger.warning(f"[Movieland] Screenings failed for {binfo['name']}: {e}")
                    continue

                await self._human_delay(0.5, 1.0)

            if on_progress:
                on_progress("סריקת הקרנות - מובילנד", len(MOVIELAND_BRANCHES),
                            len(MOVIELAND_BRANCHES),
                            f"נמצאו {len(all_screenings)} הקרנות")

            logger.info(f"[Movieland] Total screenings found: {len(all_screenings)}")

        except Exception as e:
            logger.error(f"[Movieland] scrape_screenings failed: {e}")
        finally:
            await browser.close()
            await pw.stop()

        return all_screenings

    # ── Ticket updates (seat map navigation) ─────────────────────────────

    async def scrape_ticket_updates(self, on_progress=None,
                                     on_screening_update=None) -> list[ScrapedScreening]:
        """Navigate to each screening's seat map and count seats.

        Unlike Hot Cinema, Movieland's booking links go directly to the seat map
        — no intermediate steps needed.
        """
        pw, browser, context = await self._launch_browser()
        results: list[ScrapedScreening] = []

        try:
            page = await context.new_page()

            if on_progress:
                on_progress("סורק כיסאות - מובילנד", 0, 1, "אוסף הקרנות מסניפים")

            # Collect booking URLs from all branch pages
            branch_urls = await self._discover_branch_urls(page)
            all_booking_items: list[dict] = []

            for bid, binfo in MOVIELAND_BRANCHES.items():
                url = branch_urls.get(bid)
                if not url:
                    continue

                try:
                    _, _, booking_items = await self._scrape_branch_page(
                        page, url, binfo, collect_booking_urls=True
                    )
                    all_booking_items.extend(booking_items)
                except Exception as e:
                    logger.warning(f"[Movieland] Booking URL collection failed for {binfo['name']}: {e}")
                    continue

                await self._human_delay(0.3, 0.8)

            # Deduplicate by booking URL
            seen_urls: set[str] = set()
            unique_items: list[dict] = []
            for item in all_booking_items:
                url = item["booking_url"]
                if url not in seen_urls:
                    seen_urls.add(url)
                    unique_items.append(item)

            logger.info(f"[Movieland] Collected {len(unique_items)} unique booking URLs from {len(branch_urls)} branches")

            if on_progress:
                on_progress("סורק כיסאות - מובילנד", 0, len(unique_items), "מתחיל סריקת כיסאות")

            # Navigate to each booking URL and count seats
            for idx, item in enumerate(unique_items):
                if on_progress:
                    on_progress("סורק כיסאות - מובילנד", idx + 1, len(unique_items),
                                f"{item['movie_title']} @ {item.get('branch_he', '')}")

                try:
                    await self._open_url(page, item["booking_url"], wait_for_network=True)
                    await asyncio.sleep(3)

                    total, sold, sold_positions = await self._count_seats_on_page(
                        page,
                        movie_title=item["movie_title"],
                        screening_time=item.get("time_str", ""),
                        branch_he=item.get("branch_he", ""),
                    )

                    # Try to get hall from seat map page if not already known
                    hall = item["hall"]
                    if not hall:
                        try:
                            hall_text = await page.evaluate("""() => {
                                const text = document.body ? document.body.textContent : '';
                                const m = text.match(/אולם\\s*(\\d+|[A-Za-z]+)/);
                                if (m) return m[1];
                                const m2 = text.match(/Hall\\s*(\\d+|[A-Za-z]+)/i);
                                if (m2) return m2[1];
                                return '';
                            }""")
                            if hall_text:
                                hall = hall_text
                        except Exception:
                            pass

                    screening = ScrapedScreening(
                        movie_title=item["movie_title"],
                        cinema_name=item["cinema_name"],
                        city=item["city"],
                        showtime=item["showtime"] or datetime.now(),
                        hall=hall,
                        format=item["format"],
                        tickets_sold=sold,
                        total_seats=total,
                        sold_positions=sold_positions,
                    )
                    results.append(screening)

                    if on_screening_update:
                        on_screening_update(screening)

                    logger.info(
                        f"[Movieland] '{item['movie_title']}' @ {item['cinema_name']}: "
                        f"{sold}/{total} seats sold"
                        + (f" (hall {hall})" if hall else "")
                    )

                except Exception as e:
                    logger.warning(
                        f"[Movieland] Seat map failed for '{item['movie_title']}': {e}"
                    )
                    continue

                await self._human_delay(1.0, 2.0)

        except Exception as e:
            logger.error(f"[Movieland] scrape_ticket_updates failed: {e}")
        finally:
            await browser.close()
            await pw.stop()

        logger.info(f"[Movieland] Ticket updates complete: {len(results)} screenings")
        return results

    async def close(self):
        await self.client.aclose()
