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
        "aliases": ["הצוק", "ת\"א", "ת״א"],
        "fallback_urls": [
            "https://movieland.co.il/tel-aviv/",
            "https://movieland.co.il/theater/1/tel-aviv/",
            "https://movieland.co.il/theater/1/%D7%AA%D7%9C-%D7%90%D7%91%D7%99%D7%91/",
        ],
    },
    "netanya": {
        "name": "Movieland Netanya",
        "name_he": "מובילנד נתניה",
        "city": "Netanya",
        "city_he": "נתניה",
        "slug": "netanya",
        "fallback_urls": [
            "https://movieland.co.il/netanya/",
            "https://movieland.co.il/theater/2/netanya/",
            "https://movieland.co.il/theater/2/%D7%A0%D7%AA%D7%A0%D7%99%D7%94/",
        ],
    },
    "haifa": {
        "name": "Movieland Haifa",
        "name_he": "מובילנד חיפה",
        "city": "Haifa",
        "city_he": "חיפה",
        "slug": "haifa",
        "fallback_urls": [
            "https://movieland.co.il/haifa/",
            "https://movieland.co.il/theater/3/haifa/",
            "https://movieland.co.il/theater/3/%D7%97%D7%99%D7%A4%D7%94/",
        ],
    },
    "karmiel": {
        "name": "Movieland Karmiel",
        "name_he": "מובילנד כרמיאל",
        "city": "Karmiel",
        "city_he": "כרמיאל",
        "slug": "karmiel",
        "fallback_urls": [
            "https://movieland.co.il/karmiel/",
            "https://movieland.co.il/theater/4/karmiel/",
            "https://movieland.co.il/theater/4/%D7%9B%D7%A8%D7%9E%D7%99%D7%90%D7%9C/",
        ],
    },
    "afula": {
        "name": "Movieland Afula",
        "name_he": "מובילנד עפולה",
        "city": "Afula",
        "city_he": "עפולה",
        "slug": "afula",
        "fallback_urls": [
            "https://movieland.co.il/afula/",
            "https://movieland.co.il/theater/5/afula/",
            "https://movieland.co.il/theater/5/%D7%A2%D7%A4%D7%95%D7%9C%D7%94/",
        ],
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

# Alternative names used on the Movieland website
_BRANCH_NAME_MAP["הצוק"] = MOVIELAND_BRANCHES["tel_aviv"]
_BRANCH_NAME_MAP["ת\"א"] = MOVIELAND_BRANCHES["tel_aviv"]
_BRANCH_NAME_MAP["ת״א"] = MOVIELAND_BRANCHES["tel_aviv"]


def _branch_matches(binfo: dict, text: str) -> bool:
    """Check if text matches a branch by city_he or aliases."""
    if binfo["city_he"] in text:
        return True
    for alias in binfo.get("aliases", []):
        if alias in text:
            return True
    return False


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
                "--single-process",
                "--js-flags=--max-old-space-size=512",
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

        await asyncio.sleep(1)  # Reduced from 2s — page already loaded

        # Dismiss any popup/modal that appears
        await self._dismiss_popup(page)

    async def _dismiss_popup(self, page: Page):
        """Try to dismiss popups/modals (cookie banners, promo popups, etc.)."""
        try:
            # Look for common close/dismiss buttons
            close_selectors = [
                'button[aria-label="close"]',
                'button[aria-label="Close"]',
                'button[aria-label="סגור"]',
                '.close-btn', '.close-button', '.modal-close',
                '[class*="close"]', '[class*="Close"]',
                'button.close', 'a.close',
                '[class*="popup"] button', '[class*="Popup"] button',
                '[class*="modal"] button', '[class*="Modal"] button',
                '[class*="overlay"] button',
                'button[class*="dismiss"]',
                # X button patterns
                'button:has-text("×")', 'button:has-text("✕")',
                'button:has-text("X")', 'button:has-text("x")',
                'button:has-text("סגור")', 'a:has-text("סגור")',
            ]

            for selector in close_selectors:
                try:
                    btn = await page.query_selector(selector)
                    if btn:
                        is_visible = await btn.is_visible()
                        if is_visible:
                            await btn.click()
                            logger.info(f"[Movieland] Dismissed popup via: {selector}")
                            await asyncio.sleep(1)
                            return
                except Exception:
                    continue

            # Also try pressing Escape
            try:
                await page.keyboard.press("Escape")
                await asyncio.sleep(0.5)
            except Exception:
                pass
        except Exception:
            pass

    # ── Branch discovery ─────────────────────────────────────────────────

    @staticmethod
    def _is_junk_link(href: str) -> bool:
        """Filter out links that are clearly not branch/movie pages."""
        junk_patterns = [
            'whatsapp.com', 'wa.me', 'api.whatsapp',
            'facebook.com', 'instagram.com', 'twitter.com', 'tiktok.com',
            'youtube.com', 'linkedin.com',
            'mailto:', 'tel:', 'javascript:',
            '#', '/cancel', '/ביטול', '/refund', '/החזר',
            '/contact', '/צור-קשר', '/about', '/אודות',
            '/terms', '/תקנון', '/privacy', '/פרטיות',
            '/faq', '/login', '/register', '/cart',
            '/careers', '/דרושים',
        ]
        href_lower = href.lower()
        return any(p in href_lower for p in junk_patterns)

    async def _discover_branch_urls(self, page: Page) -> dict[str, str]:
        """Discover branch page URLs from the site.

        Strategy:
        1. Try each branch's fallback_urls list — probe with a HEAD-like
           navigation and check if the page has movie content (.date-cont).
        2. Only if fallbacks fail, fall back to homepage link scanning.

        Returns {branch_key: full_url} for each known branch.
        """
        branch_urls: dict[str, str] = {}

        # ── Strategy 1: probe known fallback URLs (fast) ──────────────────
        for bid, binfo in MOVIELAND_BRANCHES.items():
            for url in binfo.get("fallback_urls", []):
                try:
                    resp = await page.request.get(url, timeout=8000)
                    if resp.status == 200:
                        branch_urls[bid] = url
                        logger.info(f"[Movieland] Branch URL (fallback probe): {binfo['name']} -> {url}")
                        break
                except Exception:
                    continue

        if len(branch_urls) >= len(MOVIELAND_BRANCHES):
            logger.info(f"[Movieland] All {len(branch_urls)} branches found via fallback URLs")
            return branch_urls

        # ── Strategy 2: scan homepage links (slower fallback) ─────────────
        logger.info(f"[Movieland] Fallback found {len(branch_urls)}/{len(MOVIELAND_BRANCHES)}, scanning homepage")
        try:
            await self._open_url(page, BASE_URL, wait_for_network=True)

            all_links = await page.query_selector_all('a[href]')
            for link in all_links:
                try:
                    href = await link.get_attribute("href") or ""
                    if self._is_junk_link(href):
                        continue
                    text = (await link.inner_text()).strip()

                    for bid2, binfo2 in MOVIELAND_BRANCHES.items():
                        if bid2 in branch_urls:
                            continue
                        if _branch_matches(binfo2, href) or _branch_matches(binfo2, text):
                            if href.startswith("/"):
                                href = f"{BASE_URL}{href}"
                            if href.startswith("http") and "movieland.co.il" in href:
                                branch_urls[bid2] = href
                                logger.info(f"[Movieland] Found branch URL (homepage): {binfo2['name']} -> {href}")
                except Exception:
                    continue
        except Exception as e:
            logger.warning(f"[Movieland] Homepage scanning failed: {e}")

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
        await asyncio.sleep(1)  # Reduced from 2s

        try:
            await page.screenshot(
                path=_debug_screenshot_path("step1_branch", branch=binfo["city_he"])
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

        try:
            await page.screenshot(
                path=_debug_screenshot_path("step2_scrolled", branch=binfo["city_he"]),
                full_page=True,
            )
        except Exception:
            pass

        # ── Dump page HTML for debugging ──────────────────────────────
        page_html = await page.content()
        try:
            html_dump_path = os.path.join(
                _DEBUG_SCREENSHOTS_DIR,
                f"mvl_{re.sub(r'[^a-zA-Z0-9]', '_', binfo.get('city', ''))}_{datetime.now().strftime('%H%M%S')}.html"
            )
            os.makedirs(_DEBUG_SCREENSHOTS_DIR, exist_ok=True)
            with open(html_dump_path, "w", encoding="utf-8") as f:
                f.write(page_html)
            logger.info(f"[Movieland] Saved branch page HTML to {html_dump_path}")
        except Exception as e:
            logger.warning(f"[Movieland] Failed to save HTML dump: {e}")

        # ── Extract movies via date-cont sections ──────────────────────
        # Movieland DOM structure: each movie is inside a .date-cont div
        # containing a.bg-theater-c (title), .bg-genre (genre/duration),
        # and order links inside .right-help > .bg-hours2
        js_movies = await page.evaluate("""() => {
            const containers = document.querySelectorAll('.date-cont');
            const movies = [];
            for (const cont of containers) {
                const titleEl = cont.querySelector('a.bg-theater-c');
                if (!titleEl) continue;
                const title = titleEl.textContent.trim();
                if (!title || title.length < 2) continue;
                const movieUrl = titleEl.getAttribute('href') || '';

                // Genre, duration, rating from .bg-genre
                const genreEl = cont.querySelector('.bg-genre');
                let genre = '', duration = 0, rating = '';
                if (genreEl) {
                    const text = genreEl.textContent.trim();
                    const parts = text.split('|').map(s => s.trim());
                    genre = parts[0] || '';
                    const durMatch = text.match(/(\\d+)\\s*ד/);
                    if (durMatch) duration = parseInt(durMatch[1]);
                    if (parts.length >= 3) rating = parts[parts.length - 1];
                }

                // Language from .bg-heady
                const headyEl = cont.querySelector('.bg-heady');
                const langText = headyEl ? headyEl.textContent.trim() : '';

                // Poster
                const posterEl = cont.querySelector('img.img-fluid');
                let posterUrl = posterEl ? (posterEl.getAttribute('src') || '') : '';

                // Order links (screenings)
                const orderLinks = cont.querySelectorAll('a[href*="/order/"]');
                const screeningList = [];
                for (const a of orderLinks) {
                    const href = a.getAttribute('href') || '';
                    if (!href.includes('eventID')) continue;
                    const timeSpan = a.querySelector('span');
                    const time = timeSpan ? timeSpan.textContent.trim() : '';
                    screeningList.push({ href, time });
                }

                movies.push({
                    title, movieUrl, genre, duration, rating,
                    langText, posterUrl, screenings: screeningList
                });
            }
            return movies;
        }""")

        logger.info(f"[Movieland] Branch '{binfo['name']}': {len(js_movies or [])} movies from date-cont sections")

        # Process JS-extracted movies into ScrapedMovie/ScrapedScreening
        for mv in (js_movies or []):
            title = mv.get("title", "").strip()
            if not title or title in seen_titles:
                continue
            if len(title) > 80 or len(title) < 2:
                continue

            seen_titles.add(title)

            # Build movie detail URL
            movie_url = mv.get("movieUrl", "")
            if movie_url.startswith("/"):
                movie_url = f"{BASE_URL}{movie_url}"

            # Parse poster URL
            poster_url = mv.get("posterUrl", "")
            if poster_url and poster_url.startswith("/"):
                poster_url = f"{BASE_URL}{poster_url}"

            movies.append(ScrapedMovie(
                title=title,
                title_he=title,
                genre=mv.get("genre", ""),
                duration_minutes=mv.get("duration", 0),
                rating=mv.get("rating", ""),
                poster_url=poster_url,
                detail_url=movie_url or branch_url,
            ))

            # Detect language from bg-heady
            lang_text = mv.get("langText", "").lower()
            language = "subtitled"
            if "מדובב" in lang_text:
                language = "dubbed"

            # Process screenings
            for scr in mv.get("screenings", []):
                time_str = scr.get("time", "")
                time_match = re.search(r'(\d{1,2}:\d{2})', time_str)
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

                fmt = "2D"
                hall = ""

                screenings.append(ScrapedScreening(
                    movie_title=title,
                    cinema_name=binfo["name"],
                    city=binfo["city"],
                    showtime=showtime,
                    hall=hall,
                    format=fmt,
                    language=language,
                ))

                if collect_booking_urls:
                    href = scr.get("href", "")
                    booking_url = href
                    if booking_url.startswith("/"):
                        booking_url = f"{BASE_URL}{booking_url}"
                    booking_items.append({
                        "movie_title": title,
                        "booking_url": booking_url,
                        "cinema_name": binfo["name"],
                        "city": binfo["city"],
                        "showtime": showtime,
                        "hall": hall,
                        "format": fmt,
                        "branch_he": binfo["city_he"],
                        "time_str": time_str,
                    })

        logger.info(
            f"[Movieland] Branch '{binfo['name']}': "
            f"{len(movies)} movies, {len(screenings)} screenings"
        )

        # Strategy 2: CSS selector-based movie section detection (fallback)
        if not movies:
            movie_sections = await page.query_selector_all(
                '[class*="movie"], [class*="Movie"], [class*="film"], [class*="Film"], '
                '[class*="event"], [class*="Event"], '
                'article, .movie-item, .film-item, .event-item'
            )

            if not movie_sections:
                movie_sections = await page.query_selector_all('h2, h3, h4')

            for section in movie_sections:
                try:
                    section_text = (await section.inner_text()).strip()
                    if not section_text or len(section_text) < 3:
                        continue

                    title = ""
                    title_el = await section.query_selector(
                        'h2, h3, h4, [class*="title"], [class*="Title"], '
                        '[class*="name"], [class*="Name"]'
                    )
                    if title_el:
                        title = (await title_el.inner_text()).strip()

                    if not title:
                        tag = await section.evaluate("el => el.tagName")
                        if tag in ("H2", "H3", "H4"):
                            title = section_text.split('\n')[0].strip()

                    if not title or len(title) < 2 or title in seen_titles:
                        continue

                    # Skip navigation/branch labels and junk titles
                    if any(binfo2["city_he"] in title for binfo2 in MOVIELAND_BRANCHES.values()):
                        continue
                    skip_words = [
                        "סניפים", "תקנון", "צור קשר", "אודות", "FAQ", "שאלות",
                        "whatsapp", "ווטסאפ", "ואטסאפ", "מובילנד", "movieland",
                        "דרושים", "careers", "קניון", "mall",
                        "הזמנת כרטיסים", "ביטול", "החזר", "תנאי",
                        "כל הזכויות", "copyright", "powered by",
                        "נגישות", "accessibility",
                    ]
                    if any(w.lower() in title.lower() for w in skip_words):
                        continue
                    if len(title) > 80:
                        continue
                    if re.match(r'^[\d\s\-_.,:;!?]+$', title):
                        continue

                    seen_titles.add(title)

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

                    # Collect showtimes for this movie
                    section_links = await section.query_selector_all('a[href]')
                    showtime_buttons = await section.query_selector_all(
                        'button, [class*="time"], [class*="hour"], [class*="show"]'
                    )

                    found_times: set[str] = set()

                    for link in section_links:
                        try:
                            href = await link.get_attribute("href") or ""
                            link_text = (await link.inner_text()).strip()

                            time_match = re.search(r'(\d{1,2}:\d{2})', link_text)
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

                            hall = ""
                            parent_text = section_text
                            hall_match = re.search(r'אולם\s*(\d+|[A-Za-z]+)', parent_text)
                            if hall_match:
                                hall = hall_match.group(1)

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

                            if collect_booking_urls:
                                is_order_link = "/order/" in href and "eventID" in href
                                is_booking_link = BOOKING_DOMAIN in href

                                if is_order_link or is_booking_link:
                                    booking_url = href
                                    if booking_url.startswith("/"):
                                        booking_url = f"{BASE_URL}{booking_url}"

                                    booking_items.append({
                                        "movie_title": title,
                                        "booking_url": booking_url,
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

            logger.info(f"[Movieland] Strategy 2 (CSS selectors): {len(movies)} movies, {len(screenings)} screenings")

        # ── Fallback: scan ALL links on the page for booking URLs ────
        all_page_links = await page.query_selector_all('a[href]')
        if collect_booking_urls and not booking_items:
            for link in all_page_links:
                try:
                    href = await link.get_attribute("href") or ""
                    # Match either biggerpicture links or /order/ links
                    is_order_link = "/order/" in href and "eventID" in href
                    is_booking_link = BOOKING_DOMAIN in href
                    if not is_order_link and not is_booking_link:
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

                    booking_url = href
                    if booking_url.startswith("/"):
                        booking_url = f"{BASE_URL}{booking_url}"

                    booking_items.append({
                        "movie_title": movie_title or "Unknown",
                        "booking_url": booking_url,
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
                    is_order_link = "/order/" in href and "eventID" in href
                    is_booking_link = BOOKING_DOMAIN in href
                    if not is_order_link and not is_booking_link:
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
                        booking_url = href
                        if booking_url.startswith("/"):
                            booking_url = f"{BASE_URL}{booking_url}"
                        booking_items.append({
                            "movie_title": movie_title or "Unknown",
                            "booking_url": booking_url,
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

    async def _click_date_tab(self, page: Page, target_date, binfo: dict) -> bool:
        """Try to click a date tab on the current branch page.

        Movieland branch pages have a date bar with:
        - Today, tomorrow, day-after-tomorrow as direct buttons
        - A dropdown/calendar button for further dates (up to ~7 days)

        For days 1-2 (tomorrow/day-after), we click the direct tab.
        For days 3+, we first open the dropdown/calendar, then click the date inside.
        """
        today = datetime.now().date()
        day_offset = (target_date - today).days

        target_str = target_date.strftime("%d/%m")
        target_str2 = target_date.strftime("%d.%m")
        target_day = str(target_date.day)
        target_day_padded = target_date.strftime("%d")
        target_iso = target_date.isoformat()

        # Hebrew day names for matching
        hebrew_days = ["ראשון", "שני", "שלישי", "רביעי", "חמישי", "שישי", "שבת"]
        target_weekday_he = hebrew_days[target_date.weekday()]

        # Use JS to find and click the matching date element
        clicked = await page.evaluate("""(opts) => {
            const { targetStr, targetStr2, targetDay, targetDayPadded, targetIso,
                    dayOffset, targetWeekdayHe } = opts;

            // Helper: dispatch a proper click event (works with React/Vue/Angular)
            function realClick(el) {
                el.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
            }

            // Strategy A: elements with data-date attribute
            const dataDateEls = document.querySelectorAll('[data-date]');
            for (const el of dataDateEls) {
                const dd = el.getAttribute('data-date') || '';
                if (dd.includes(targetIso) || dd.includes(targetStr) || dd.includes(targetStr2)) {
                    realClick(el);
                    return 'data-date';
                }
            }

            // Strategy B: Direct date tabs in the date bar
            // Look for small navigation elements (not movie containers) that match the date
            const navSelectors = [
                '.day-tab', '.date-tab', '.swiper-slide',
                '[class*="day-item"]', '[class*="date-item"]',
                '[class*="dayTab"]', '[class*="dateTab"]',
                '[class*="day-btn"]', '[class*="date-btn"]',
                '.days-bar a', '.days-bar button', '.days-bar li',
                '.calendar-bar a', '.calendar-bar button',
                // Movieland-specific: look inside navigation/header area
                'nav a', 'nav button', 'header a', 'header button',
            ];
            for (const sel of navSelectors) {
                try {
                    const els = document.querySelectorAll(sel);
                    for (const el of els) {
                        if (el.closest('.date-cont')) continue;
                        const text = el.textContent.trim();
                        if (text.includes(targetStr) || text.includes(targetStr2) ||
                            (text.match(/^\\d{1,2}$/) && text === targetDay)) {
                            realClick(el);
                            return 'tab-' + sel;
                        }
                    }
                } catch(e) {}
            }

            // Strategy C: Look for all small clickable elements with date text
            // Match by: date string, day number with Hebrew weekday, or just day number
            const candidates = document.querySelectorAll(
                'a, button, [role="tab"], [role="button"], span[onclick], div[onclick], '
                + 'li, label, [class*="day"], [class*="date"], [class*="Day"], [class*="Date"]'
            );
            for (const el of candidates) {
                if (el.closest('.date-cont')) continue;
                if (el.offsetHeight > 100 || el.offsetHeight < 5) continue;
                if (el.offsetWidth > 300) continue;
                const text = el.textContent.trim();
                if (text.length > 40 || text.length === 0) continue;

                // Match date strings
                if (text.includes(targetStr) || text.includes(targetStr2)) {
                    realClick(el);
                    return 'candidate-date-str';
                }
                // Match Hebrew weekday + day number
                if (text.includes(targetWeekdayHe) && text.includes(targetDay)) {
                    realClick(el);
                    return 'candidate-weekday-he';
                }
                // Match just the day number (only for compact elements)
                if (text.length < 5 && text.match(/^\\d{1,2}$/) && text === targetDay) {
                    realClick(el);
                    return 'candidate-day-num';
                }
                // Match padded day (e.g. "02")
                if (text.length < 5 && text === targetDayPadded) {
                    realClick(el);
                    return 'candidate-day-padded';
                }
            }

            return null;
        }""", {
            "targetStr": target_str,
            "targetStr2": target_str2,
            "targetDay": target_day,
            "targetDayPadded": target_day_padded,
            "targetIso": target_iso,
            "dayOffset": day_offset,
            "targetWeekdayHe": target_weekday_he,
        })

        if clicked:
            logger.info(f"[Movieland] Clicked date tab for {target_date} via {clicked} "
                        f"(branch: {binfo['name']})")
            await asyncio.sleep(2)
            return True

        # Strategy D: For dates beyond the visible tabs, try to open a
        # dropdown/calendar first, then click the target date inside it
        if day_offset >= 3:
            logger.debug(f"[Movieland] Trying dropdown/calendar for day {day_offset}")
            opened = await self._open_date_dropdown(page, target_date, binfo)
            if opened:
                return True

        return False

    async def _open_date_dropdown(self, page: Page, target_date, binfo: dict) -> bool:
        """Try to open a date dropdown/calendar and click the target date.

        Movieland date bar has a dropdown button (often the last item) that
        reveals a calendar widget for dates beyond the first 3 visible tabs.
        """
        target_day = str(target_date.day)
        target_str = target_date.strftime("%d/%m")
        target_iso = target_date.isoformat()

        # Step 1: Find and click the dropdown trigger ("תאריכים נוספים" button)
        dropdown_opened = await page.evaluate("""() => {
            function realClick(el) {
                el.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
            }

            // Strategy 1: Look for the "תאריכים נוספים" button by text content
            const allClickable = document.querySelectorAll('a, button, div, span, [role="button"]');
            for (const el of allClickable) {
                if (el.closest('.date-cont')) continue;
                const text = el.textContent.trim();
                if (text.includes('תאריכים נוספים') || text.includes('תאריכים') ||
                    text.includes('לוח שנה') || text.includes('עוד תאריכים')) {
                    if (el.offsetHeight > 5 && el.offsetHeight < 100) {
                        realClick(el);
                        return 'text-תאריכים-נוספים';
                    }
                }
            }

            // Strategy 2: Look for elements with calendar-related classes
            const selectors = [
                '[class*="dropdown"]', '[class*="more"]', '[class*="calendar"]',
                '[class*="Dropdown"]', '[class*="More"]', '[class*="Calendar"]',
                '[class*="picker"]', '[class*="Picker"]',
                '[class*="additional"]', '[class*="extra"]',
            ];
            for (const sel of selectors) {
                try {
                    const els = document.querySelectorAll(sel);
                    for (const el of els) {
                        if (el.closest('.date-cont')) continue;
                        if (el.offsetHeight > 100 || el.offsetHeight < 5) continue;
                        realClick(el);
                        return 'dropdown-' + sel;
                    }
                } catch(e) {}
            }

            // Strategy 3: Find the leftmost element in the date bar
            // (RTL layout: "תאריכים נוספים" is the leftmost/last item)
            const firstMovie = document.querySelector('.date-cont');
            if (firstMovie) {
                const beforeMovie = [];
                for (const el of allClickable) {
                    if (el.closest('.date-cont')) continue;
                    const rect = el.getBoundingClientRect();
                    const movieRect = firstMovie.getBoundingClientRect();
                    if (rect.bottom < movieRect.top && rect.height < 80 && rect.height > 10) {
                        beforeMovie.push({ el, left: rect.left });
                    }
                }
                // In RTL, the "more dates" button is leftmost
                if (beforeMovie.length > 0) {
                    beforeMovie.sort((a, b) => a.left - b.left);
                    realClick(beforeMovie[0].el);
                    return 'leftmost-before-movies';
                }
            }

            return null;
        }""")

        if not dropdown_opened:
            return False

        logger.debug(f"[Movieland] Opened date dropdown via {dropdown_opened}")
        await asyncio.sleep(1)

        # Step 2: Now look for the target date inside the opened dropdown/calendar
        clicked = await page.evaluate("""(opts) => {
            const { targetDay, targetStr, targetIso } = opts;

            function realClick(el) {
                el.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
            }

            // Look for newly visible elements (popover, dropdown, calendar)
            const popoverSelectors = [
                '[class*="popover"]', '[class*="dropdown-menu"]', '[class*="calendar"]',
                '[class*="datepicker"]', '[class*="picker"]', '[class*="popup"]',
                '[class*="Popover"]', '[class*="Dropdown"]', '[class*="Calendar"]',
                '[class*="modal"]', '[class*="overlay"]',
            ];

            for (const sel of popoverSelectors) {
                try {
                    const containers = document.querySelectorAll(sel);
                    for (const container of containers) {
                        if (container.offsetHeight < 5) continue;
                        // Look for clickable date elements inside
                        const clickables = container.querySelectorAll('a, button, td, span, div, li');
                        for (const el of clickables) {
                            const text = el.textContent.trim();
                            if (text === targetDay || text.includes(targetStr)) {
                                realClick(el);
                                return 'calendar-' + sel;
                            }
                        }
                    }
                } catch(e) {}
            }

            // Also try: any newly visible small element with the target day
            const allSmall = document.querySelectorAll('td, [class*="day"], [class*="cell"]');
            for (const el of allSmall) {
                if (el.offsetHeight < 5 || el.offsetHeight > 60) continue;
                const text = el.textContent.trim();
                if (text === targetDay) {
                    realClick(el);
                    return 'calendar-cell';
                }
            }

            return null;
        }""", {
            "targetDay": target_day,
            "targetStr": target_str,
            "targetIso": target_iso,
        })

        if clicked:
            logger.info(f"[Movieland] Clicked date in dropdown for {target_date} via {clicked} "
                        f"(branch: {binfo['name']})")
            await asyncio.sleep(2)
            return True

        return False

    async def _extract_event_ids(self, page: Page) -> set[str]:
        """Extract eventID values from all order links on the current page."""
        try:
            ids = await page.evaluate("""() => {
                const links = document.querySelectorAll('a[href*="/order/"]');
                const ids = [];
                for (const a of links) {
                    const m = (a.getAttribute('href') || '').match(/eventID=([^&]+)/);
                    if (m) ids.push(m[1]);
                }
                return ids;
            }""")
            return set(ids or [])
        except Exception:
            return set()

    async def _navigate_branch_calendar(self, page: Page, branch_url: str,
                                         binfo: dict, days: int = 7,
                                         collect_booking_urls: bool = False,
                                         ) -> tuple[list[ScrapedMovie], list[ScrapedScreening], list[dict]]:
        """Scrape a branch page across multiple days.

        Approach (in priority order for each day):
        1. Click a date tab/button on the branch page date bar.
        2. Reload the branch URL with a date query parameter as fallback.
        3. Validate navigation actually changed the page via eventID comparison.
        """
        all_movies: list[ScrapedMovie] = []
        all_screenings: list[ScrapedScreening] = []
        all_booking_items: list[dict] = []
        seen_titles: set[str] = set()
        seen_screening_keys: set[str] = set()  # dedup safety: (movie_title, showtime)

        # First, scrape the current day (default view)
        movies, screenings, bookings = await self._scrape_branch_page(
            page, branch_url, binfo, collect_booking_urls
        )
        all_movies.extend(movies)
        all_screenings.extend(screenings)
        all_booking_items.extend(bookings)
        for m in movies:
            seen_titles.add(m.title)

        # Fingerprint day-0 for duplicate detection
        day0_event_ids = await self._extract_event_ids(page)
        for scr in screenings:
            key = f"{scr.movie_title}|{scr.showtime}"
            seen_screening_keys.add(key)

        day0_screening_count = len(screenings)
        logger.info(f"[Movieland] Branch '{binfo['name']}' day 0: "
                    f"{len(movies)} movies, {day0_screening_count} screenings, "
                    f"{len(day0_event_ids)} unique eventIDs")

        # Log date bar diagnostics once per branch
        try:
            date_bar_info = await page.evaluate("""() => {
                const info = { elements: [], dateTexts: [] };
                // Look for date-related navigation elements
                const selectors = [
                    '[class*="date"]', '[class*="day"]', '[class*="calendar"]',
                    '[class*="swiper"]', '.days-bar', '.calendar-bar',
                    '[class*="Date"]', '[class*="Day"]',
                ];
                for (const sel of selectors) {
                    try {
                        const els = document.querySelectorAll(sel);
                        for (const el of els) {
                            if (el.closest('.date-cont')) continue;
                            if (el.offsetHeight > 200) continue;
                            const text = el.textContent.trim().substring(0, 60);
                            if (text.length > 0 && text.length < 60) {
                                info.elements.push({
                                    sel, tag: el.tagName,
                                    cls: (el.className || '').substring(0, 80),
                                    text, h: el.offsetHeight, w: el.offsetWidth,
                                });
                            }
                        }
                    } catch(e) {}
                }
                return info;
            }""")
            if date_bar_info and date_bar_info.get("elements"):
                logger.info(f"[Movieland] Date bar elements for {binfo['name']}: "
                            f"{date_bar_info['elements'][:10]}")
        except Exception as e:
            logger.debug(f"[Movieland] Date bar diagnostics failed: {e}")

        # Try to navigate to subsequent days
        consecutive_empty = 0
        for day_offset in range(1, days):
            target_date = datetime.now().date() + timedelta(days=day_offset)

            try:
                navigated = False

                # Method 1: Click date tab on the branch page (preferred)
                # Make sure we're on the branch page
                try:
                    current_url = page.url
                    if 'movieland.co.il' not in current_url:
                        await self._open_url(page, branch_url, wait_for_network=True)
                        await asyncio.sleep(1)
                except Exception:
                    await self._open_url(page, branch_url, wait_for_network=True)
                    await asyncio.sleep(1)

                navigated = await self._click_date_tab(page, target_date, binfo)

                if navigated:
                    # Validate: check that eventIDs actually changed
                    new_event_ids = await self._extract_event_ids(page)
                    if new_event_ids and new_event_ids == day0_event_ids:
                        logger.debug(f"[Movieland] Date tab click for day {day_offset} "
                                     f"did not change content (same eventIDs), treating as failed")
                        navigated = False

                # Method 2: URL with date parameter (fallback)
                if not navigated:
                    for date_param_fmt in [
                        ("date", target_date.strftime("%d/%m/%Y")),
                        ("date", target_date.strftime("%Y-%m-%d")),
                        ("day", target_date.strftime("%Y-%m-%d")),
                    ]:
                        param_name, param_val = date_param_fmt
                        sep = "&" if "?" in branch_url else "?"
                        day_url = f"{branch_url}{sep}{param_name}={param_val}"
                        try:
                            await self._open_url(page, day_url, wait_for_network=True)
                            # Validate: eventIDs must differ from day 0
                            new_event_ids = await self._extract_event_ids(page)
                            has_content = len(new_event_ids) > 0
                            is_different = new_event_ids != day0_event_ids

                            if has_content and is_different:
                                navigated = True
                                logger.info(f"[Movieland] Day {day_offset} via URL param "
                                            f"{param_name}={param_val} "
                                            f"({len(new_event_ids)} eventIDs)")
                                break
                        except Exception:
                            continue

                if not navigated:
                    logger.debug(f"[Movieland] Could not navigate to day {day_offset} "
                                 f"({target_date}) for {binfo['name']}")
                    consecutive_empty += 1
                    if consecutive_empty >= 3:
                        logger.info(f"[Movieland] Stopping calendar nav for {binfo['name']} "
                                    f"after {consecutive_empty} consecutive failures")
                        break
                    continue

                consecutive_empty = 0

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

                # Dedup screenings: skip if (movie_title, showtime) already seen
                new_count = 0
                for scr in day_screenings:
                    key = f"{scr.movie_title}|{scr.showtime}"
                    if key not in seen_screening_keys:
                        seen_screening_keys.add(key)
                        all_screenings.append(scr)
                        new_count += 1
                    else:
                        logger.debug(f"[Movieland] Skipping duplicate screening: {key}")

                all_booking_items.extend(day_bookings)

                logger.info(f"[Movieland] Branch '{binfo['name']}' day {day_offset} "
                            f"({target_date}): {new_count} new screenings"
                            + (f" ({len(day_screenings) - new_count} duplicates skipped)"
                               if new_count < len(day_screenings) else ""))

            except Exception as e:
                logger.debug(f"[Movieland] Calendar navigation failed for day {day_offset}: {e}")
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    break
                continue

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

        # Wait for Angular SPA to render seat elements (up to 12s)
        for _wait in range(24):
            has_seats = await page.evaluate("""() => {
                // Check for Movieland seat images
                const imgs = document.querySelectorAll('image[href*="mvl-seat"], [style*="mvl-seat"]');
                if (imgs.length > 0) return true;
                // Check for any seat-like elements
                const seats = document.querySelectorAll('[class*="seat"]');
                if (seats.length > 5) return true;
                // Check if app-root has rendered content (not just loading spinner)
                const appRoot = document.querySelector('app-root');
                if (appRoot && appRoot.children.length > 1) return true;
                return false;
            }""")
            if has_seats:
                break
            await asyncio.sleep(0.5)
        else:
            logger.warning(f"[Movieland] Seat elements did not appear after 12s wait")

        seat_data = await page.evaluate("""() => {
            // ===== METHOD A: IMAGE-BASED DETECTION (Movieland specific) =====
            // Phase 1: Collect all seat candidates into an array
            const imgCandidates = [];
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

                imgCandidates.push({
                    top: bbox.top, left: bbox.left,
                    width: bbox.width, height: bbox.height,
                    isSold: src.includes('/unavailable/'),
                    isAvail: src.includes('/available/'),
                    tag: el.tagName,
                    href: href.substring(0, 80),
                    bgImg: bgImg.substring(0, 80),
                });
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

                imgCandidates.push({
                    top: bbox.top, left: bbox.left,
                    width: bbox.width, height: bbox.height,
                    isSold: bgImg.includes('/unavailable/'),
                    isAvail: !bgImg.includes('/unavailable/'),
                    tag: el.tagName,
                    href: '',
                    bgImg: bgImg.substring(0, 80),
                });
            }

            // Phase 2: Legend exclusion via Y-position gap
            const ys = imgCandidates.map(c => c.top).sort((a, b) => a - b);
            let maxGap = 0, gapY = Infinity;
            for (let i = 1; i < ys.length; i++) {
                const gap = ys[i] - ys[i - 1];
                if (gap > maxGap) { maxGap = gap; gapY = ys[i]; }
            }
            const cutoffY = maxGap > 150 ? gapY : Infinity;

            // Phase 3: Count seats, excluding legend elements below cutoff
            let imgTotalCount = 0, imgSoldCount = 0, cutByLegend = 0;
            const imgSoldPositions = [];
            const imgSamples = [];
            const imgExcludedSample = [];

            for (const c of imgCandidates) {
                if (c.top >= cutoffY) {
                    cutByLegend++;
                    if (imgExcludedSample.length < 5) {
                        imgExcludedSample.push({
                            tag: c.tag, y: Math.round(c.top),
                            x: Math.round(c.left), reason: 'legend_cutoff',
                        });
                    }
                    continue;
                }
                imgTotalCount++;
                if (c.isSold) {
                    imgSoldCount++;
                    imgSoldPositions.push([Math.round(c.left / 5), Math.round(c.top / 5)]);
                }
                if (imgSamples.length < 15) {
                    imgSamples.push({
                        tag: c.tag,
                        href: c.href,
                        bgImg: c.bgImg,
                        w: Math.round(c.width),
                        h: Math.round(c.height),
                        x: Math.round(c.left),
                        y: Math.round(c.top),
                        status: c.isSold ? 'sold' : (c.isAvail ? 'available' : 'unknown'),
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
                    total: imgTotalCount,
                    sold: imgSoldCount,
                    soldPositions: imgSoldPositions,
                    samples: imgSamples,
                    cutByLegend,
                    cutoffY: cutoffY === Infinity ? 'none' : Math.round(cutoffY),
                    maxGap: Math.round(maxGap),
                    excludedSample: imgExcludedSample,
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

        # Take pre-annotation debug screenshot
        try:
            await page.screenshot(path=_debug_screenshot_path(
                "step4_seats", movie=movie_title, branch=branch_he,
                time_str=screening_time,
            ))
        except Exception:
            pass

        # ── Annotate seats for visual debugging ──────────────────────
        # Add colored borders: green=available, red=sold, blue=legend, orange=unknown
        try:
            await page.evaluate("""() => {
                const seen = new Set();
                const candidates = [];

                // Collect all mvl-seat image elements
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

                    const posKey = Math.round(bbox.left) + ',' + Math.round(bbox.top);
                    if (seen.has(posKey)) continue;
                    seen.add(posKey);

                    candidates.push({
                        top: bbox.top, left: bbox.left,
                        width: bbox.width, height: bbox.height,
                        isSold: src.includes('/unavailable/'),
                        isAvail: src.includes('/available/'),
                        type: 'img',
                    });
                }

                // Also check background-image elements
                const allEls = document.querySelectorAll('*');
                for (const el of allEls) {
                    if (el.tagName === 'image' || el.tagName === 'IMAGE') continue;
                    const style = window.getComputedStyle(el);
                    const bgImg = style.backgroundImage || '';
                    if (!bgImg.includes('mvl-seat')) continue;

                    const bbox = el.getBoundingClientRect();
                    if (bbox.width < 5 || bbox.height < 5) continue;

                    const posKey = Math.round(bbox.left) + ',' + Math.round(bbox.top);
                    if (seen.has(posKey)) continue;
                    seen.add(posKey);

                    candidates.push({
                        top: bbox.top, left: bbox.left,
                        width: bbox.width, height: bbox.height,
                        isSold: bgImg.includes('/unavailable/'),
                        isAvail: !bgImg.includes('/unavailable/'),
                        type: 'bg',
                    });
                }

                // Compute legend cutoff (same Y-gap algorithm as counting)
                const ys = candidates.map(c => c.top).sort((a, b) => a - b);
                let maxGap = 0, gapY = Infinity;
                for (let i = 1; i < ys.length; i++) {
                    const gap = ys[i] - ys[i - 1];
                    if (gap > maxGap) { maxGap = gap; gapY = ys[i]; }
                }
                const cutoffY = maxGap > 150 ? gapY : Infinity;

                // Draw overlays with legend elements in blue
                for (const c of candidates) {
                    const isLegend = c.top >= cutoffY;
                    const borderColor = isLegend ? '#0088ff'
                        : c.isSold ? '#ff0000'
                        : c.isAvail ? '#00ff00'
                        : '#ff8800';
                    const div = document.createElement('div');
                    div.style.cssText = `position:fixed;left:${c.left}px;top:${c.top}px;`
                        + `width:${c.width}px;height:${c.height}px;`
                        + `border:2px solid ${borderColor};`
                        + `pointer-events:none;z-index:99999;box-sizing:border-box;`;
                    div.className = '_mvl_seat_debug_overlay';
                    document.body.appendChild(div);
                }

                // Also annotate class-based seat elements
                for (const el of allEls) {
                    const cls = (typeof el.className === 'string' ? el.className
                        : el.className && el.className.baseVal ? el.className.baseVal
                        : el.getAttribute('class') || '').toLowerCase();

                    if (!cls.includes('seat')) continue;

                    const isAvailable = cls.includes('available') || cls.includes('free');
                    const isSold = cls.includes('sold') || cls.includes('occupied')
                        || cls.includes('taken') || cls.includes('unavailable')
                        || cls.includes('reserved') || cls.includes('booked');

                    if (!isAvailable && !isSold) continue;

                    const bbox = el.getBoundingClientRect();
                    if (bbox.width < 5 || bbox.height < 5) continue;

                    const posKey = Math.round(bbox.left) + ',' + Math.round(bbox.top);
                    if (seen.has(posKey)) continue;
                    seen.add(posKey);

                    const borderColor = isSold ? '#ff0000' : '#00ff00';
                    const div = document.createElement('div');
                    div.style.cssText = `position:fixed;left:${bbox.left}px;top:${bbox.top}px;`
                        + `width:${bbox.width}px;height:${bbox.height}px;`
                        + `border:2px solid ${borderColor};`
                        + `pointer-events:none;z-index:99999;box-sizing:border-box;`;
                    div.className = '_mvl_seat_debug_overlay';
                    document.body.appendChild(div);
                }
            }""")
        except Exception as e:
            logger.debug(f"[Movieland] Seat annotation failed: {e}")

        # Take annotated screenshot (step 5) - overlays show detected seats
        try:
            await page.screenshot(path=_debug_screenshot_path(
                "step5_annotated", movie=movie_title, branch=branch_he,
                time_str=screening_time,
            ))
            logger.info("[Movieland] Step 5 screenshot saved (annotated seats)")
        except Exception:
            pass

        # Clean up overlay divs
        try:
            await page.evaluate("""() => {
                document.querySelectorAll('._mvl_seat_debug_overlay').forEach(el => el.remove());
            }""")
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
            if img.get("cutByLegend", 0) > 0:
                logger.info(
                    f"[Movieland] Legend filter: excluded {img['cutByLegend']} seats "
                    f"(cutoffY={img.get('cutoffY', '?')}, maxGap={img.get('maxGap', '?')})"
                )
                if img.get("excludedSample"):
                    logger.debug(f"[Movieland] Excluded by legend: {img['excludedSample']}")

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
        """Scrape movie catalog by visiting each branch page (parallel)."""
        pw, browser, context = await self._launch_browser()
        all_movies: list[ScrapedMovie] = []
        seen_titles: set[str] = set()

        try:
            PAGE_POOL_SIZE = 4
            pages = [await context.new_page() for _ in range(PAGE_POOL_SIZE)]

            if on_progress:
                on_progress("סריקת סרטים - מובילנד", 0, len(MOVIELAND_BRANCHES), "מחפש סניפים")

            branch_urls = await self._discover_branch_urls(pages[0])

            # Parallel branch scraping
            branch_items = list(MOVIELAND_BRANCHES.items())
            branch_sem = asyncio.Semaphore(PAGE_POOL_SIZE)

            async def _scrape_branch(idx, bid, binfo):
                url = branch_urls.get(bid)
                if not url:
                    logger.warning(f"[Movieland] No URL for branch {binfo['name']}, skipping")
                    return []
                async with branch_sem:
                    p = pages[idx % PAGE_POOL_SIZE]
                    movies, _, _ = await self._scrape_branch_page(p, url, binfo)
                    return movies

            branch_results = await asyncio.gather(*[
                _scrape_branch(i, bid, binfo)
                for i, (bid, binfo) in enumerate(branch_items)
            ], return_exceptions=True)

            for idx, result in enumerate(branch_results):
                if on_progress:
                    on_progress("סריקת סרטים - מובילנד", idx + 1,
                                len(MOVIELAND_BRANCHES), branch_items[idx][1]["name_he"])
                if isinstance(result, Exception):
                    logger.warning(f"[Movieland] Branch {branch_items[idx][1]['name']} failed: {result}")
                    continue
                for m in result:
                    if m.title not in seen_titles:
                        seen_titles.add(m.title)
                        all_movies.append(m)

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
        """Scrape screening schedule from all branch pages with calendar navigation (parallel)."""
        pw, browser, context = await self._launch_browser()
        all_screenings: list[ScrapedScreening] = []

        try:
            PAGE_POOL_SIZE = 4
            pages = [await context.new_page() for _ in range(PAGE_POOL_SIZE)]

            if on_progress:
                on_progress("סריקת הקרנות - מובילנד", 0, len(MOVIELAND_BRANCHES), "מחפש סניפים")

            branch_urls = await self._discover_branch_urls(pages[0])

            # Parallel branch calendar scraping
            branch_items = list(MOVIELAND_BRANCHES.items())
            branch_sem = asyncio.Semaphore(PAGE_POOL_SIZE)

            async def _scrape_branch_cal(idx, bid, binfo):
                url = branch_urls.get(bid)
                if not url:
                    logger.warning(f"[Movieland] No URL for branch {binfo['name']}, skipping")
                    return []
                async with branch_sem:
                    p = pages[idx % PAGE_POOL_SIZE]
                    _, screenings, _ = await self._navigate_branch_calendar(
                        p, url, binfo, days=7
                    )
                    return screenings

            branch_results = await asyncio.gather(*[
                _scrape_branch_cal(i, bid, binfo)
                for i, (bid, binfo) in enumerate(branch_items)
            ], return_exceptions=True)

            for idx, result in enumerate(branch_results):
                if on_progress:
                    on_progress("סריקת הקרנות - מובילנד", idx + 1,
                                len(MOVIELAND_BRANCHES), branch_items[idx][1]["name_he"])
                if isinstance(result, Exception):
                    logger.warning(f"[Movieland] Screenings failed for {branch_items[idx][1]['name']}: {result}")
                    continue
                all_screenings.extend(result)

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
        """Navigate to each screening's seat map and count seats (parallel).

        Movieland uses two types of booking URLs:
        1. /order/?eventID=X&theaterId=Y → redirects to ecom.biggerpicture.ai seat map
        2. Direct ecom.biggerpicture.ai links (rare)

        Both end up on the BiggerPicture seat map page where we count seats.
        """
        pw, browser, context = await self._launch_browser()
        results: list[ScrapedScreening] = []

        try:
            PAGE_POOL_SIZE = 4
            SEAT_POOL_SIZE = 4
            pages = [await context.new_page() for _ in range(PAGE_POOL_SIZE)]

            if on_progress:
                on_progress("סורק כיסאות - מובילנד", 0, 1, "אוסף הקרנות מסניפים")

            # Phase 1: Collect booking URLs from all branch pages (parallel)
            branch_urls = await self._discover_branch_urls(pages[0])
            all_booking_items: list[dict] = []
            branch_sem = asyncio.Semaphore(PAGE_POOL_SIZE)

            branch_items = [(bid, binfo) for bid, binfo in MOVIELAND_BRANCHES.items()
                            if branch_urls.get(bid)]

            async def _collect_branch(idx, bid, binfo):
                async with branch_sem:
                    p = pages[idx % PAGE_POOL_SIZE]
                    url = branch_urls[bid]
                    _, _, booking_items = await self._scrape_branch_page(
                        p, url, binfo, collect_booking_urls=True
                    )
                    return booking_items

            branch_results = await asyncio.gather(*[
                _collect_branch(i, bid, binfo)
                for i, (bid, binfo) in enumerate(branch_items)
            ], return_exceptions=True)

            for idx, result in enumerate(branch_results):
                if isinstance(result, Exception):
                    logger.warning(f"[Movieland] Booking URL collection failed for {branch_items[idx][1]['name']}: {result}")
                    continue
                all_booking_items.extend(result)

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

            # Phase 2: Navigate to each booking URL and count seats (parallel)
            seat_pages = [await context.new_page() for _ in range(SEAT_POOL_SIZE)]
            seat_sem = asyncio.Semaphore(SEAT_POOL_SIZE)
            screening_counter = 0

            async def _check_seats(task_idx, item):
                nonlocal screening_counter
                async with seat_sem:
                    screening_counter += 1
                    if on_progress:
                        on_progress("סורק כיסאות - מובילנד", screening_counter, len(unique_items),
                                    f"{item['movie_title']} @ {item.get('branch_he', '')}")

                    p = seat_pages[task_idx % SEAT_POOL_SIZE]
                    booking_url = item["booking_url"]
                    is_order_url = "/order/" in booking_url and "eventID" in booking_url

                    if is_order_url:
                        logger.info(f"[Movieland] Navigating to order URL: {booking_url}")
                        try:
                            await p.goto(booking_url, wait_until="domcontentloaded", timeout=30000)
                        except Exception as e:
                            logger.warning(f"[Movieland] Order URL navigation: {e}")

                        try:
                            await p.screenshot(path=_debug_screenshot_path(
                                "step3_order",
                                movie=item["movie_title"],
                                branch=item.get("branch_he", ""),
                                time_str=item.get("time_str", ""),
                            ))
                        except Exception:
                            pass

                        for _ in range(10):
                            if BOOKING_DOMAIN in p.url:
                                break
                            await asyncio.sleep(1)

                        if BOOKING_DOMAIN not in p.url:
                            logger.warning(
                                f"[Movieland] Order URL did not redirect to {BOOKING_DOMAIN}. "
                                f"Current URL: {p.url}"
                            )
                            try:
                                await p.screenshot(path=_debug_screenshot_path(
                                    "redirect_fail",
                                    movie=item["movie_title"],
                                    branch=item.get("branch_he", ""),
                                ))
                            except Exception:
                                pass
                            return None

                        logger.info(f"[Movieland] Redirected to: {p.url}")
                        await asyncio.sleep(2)  # Reduced from 3s

                        try:
                            await p.screenshot(path=_debug_screenshot_path(
                                "step3_redirect",
                                movie=item["movie_title"],
                                branch=item.get("branch_he", ""),
                                time_str=item.get("time_str", ""),
                            ))
                        except Exception:
                            pass
                    else:
                        await self._open_url(p, booking_url, wait_for_network=True)
                        await asyncio.sleep(2)  # Reduced from 3s

                    total, sold, sold_positions = await self._count_seats_on_page(
                        p,
                        movie_title=item["movie_title"],
                        screening_time=item.get("time_str", ""),
                        branch_he=item.get("branch_he", ""),
                    )

                    hall = item["hall"]
                    if not hall:
                        try:
                            hall_text = await p.evaluate("""() => {
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

                    return item, total, sold, sold_positions, hall

            seat_results = await asyncio.gather(*[
                _check_seats(i, item)
                for i, item in enumerate(unique_items)
            ], return_exceptions=True)

            for result in seat_results:
                if isinstance(result, Exception):
                    logger.warning(f"[Movieland] Seat task failed: {result}")
                    continue
                if result is None:
                    continue

                item, total, sold, sold_positions, hall = result
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
            logger.error(f"[Movieland] scrape_ticket_updates failed: {e}")
        finally:
            await browser.close()
            await pw.stop()

        logger.info(f"[Movieland] Ticket updates complete: {len(results)} screenings")
        return results

    async def close(self):
        await self.client.aclose()
