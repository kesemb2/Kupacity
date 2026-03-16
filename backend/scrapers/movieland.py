"""
Scraper for Movieland cinema chain using Playwright.

Movieland uses the BiggerPicture (ecom.biggerpicture.ai) platform for ticket
booking.  Clicking a showtime on movieland.co.il opens the seat-map page
directly — no intermediate quantity / continue steps.

URL patterns:
- Main site:     https://movieland.co.il/
- Movie page:    https://movieland.co.il/{slug}/  (Hebrew slug)
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


def _debug_screenshot_path(step: str, detail: str = "") -> str:
    os.makedirs(_DEBUG_SCREENSHOTS_DIR, exist_ok=True)
    ts = datetime.now().strftime("%H%M%S")
    safe = re.sub(r'[^\w\u0590-\u05FF -]', '', detail)[:30].strip().replace(' ', '_')
    parts = ["mvl", step]
    if safe:
        parts.append(safe)
    parts.append(ts)
    return os.path.join(_DEBUG_SCREENSHOTS_DIR, "_".join(parts) + ".png")


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
                        take_debug_screenshot: bool = False,
                        wait_for_network: bool = False):
        await self._human_delay()
        try:
            wait_until = "networkidle" if wait_for_network else "domcontentloaded"
            await page.goto(url, wait_until=wait_until, timeout=45000)
        except Exception as e:
            logger.warning(f"[Movieland] Page load timeout for {url}: {e}")

        await asyncio.sleep(2)

        if take_debug_screenshot:
            try:
                await page.screenshot(path=_debug_screenshot_path("page", url[:40]))
                logger.info("[Movieland] Debug screenshot saved")
            except Exception:
                pass

    # ── Seat counting (BiggerPicture seat map) ───────────────────────────

    async def _count_seats_on_page(self, page: Page,
                                    movie_title: str = "",
                                    screening_time: str = "") -> tuple[int, int, list]:
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
            // Movieland uses <image> elements with href containing /mvl-seat/
            const imgTotal = { count: 0 };
            const imgSold = { count: 0 };
            const imgSoldPositions = [];
            const imgSamples = [];
            const seen = new Set();

            // Check all <image> elements in SVG
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

                // Deduplicate by position
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
                if (el.tagName === 'image' || el.tagName === 'IMAGE') continue; // already checked
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

            // Debug: get page HTML snippet
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
                bodyHTML,
            };
        }""")

        # Take debug screenshot
        try:
            await page.screenshot(path=_debug_screenshot_path("seats", movie_title))
        except Exception:
            pass

        if seat_data:
            img = seat_data.get("imageMethod", {})
            cls = seat_data.get("classMethod", {})

            logger.warning(
                f"[Movieland] Seats IMAGE method: {img.get('total', 0)} total, "
                f"{img.get('sold', 0)} sold | samples: {img.get('samples', [])[:3]}"
            )
            logger.warning(
                f"[Movieland] Seats CLASS method: {cls.get('total', 0)} total, "
                f"{cls.get('sold', 0)} sold | samples: {cls.get('samples', [])[:3]}"
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
                    logger.warning(f"[Movieland] Page HTML (first 1000): {html[:1000]}")

        return total, sold, sold_positions

    # ── Movie discovery ──────────────────────────────────────────────────

    async def scrape_movies(self, on_progress=None) -> list[ScrapedMovie]:
        """Scrape movie catalog from movieland.co.il."""
        pw, browser, context = await self._launch_browser()
        movies: list[ScrapedMovie] = []
        seen_titles: set[str] = set()

        try:
            page = await context.new_page()

            if on_progress:
                on_progress("סריקת סרטים - מובילנד", 0, 1, "טוען אתר")

            await self._open_url(page, BASE_URL, take_debug_screenshot=True,
                                 wait_for_network=True)

            # Try to find movie listings - look for common patterns
            # Strategy 1: Look for links to movie pages
            movie_links = await page.query_selector_all(
                'a[href*="/movie/"], a[href*="/seret/"], a[href*="/film/"]'
            )

            # Strategy 2: Look for movie cards/items with links
            if not movie_links:
                movie_links = await page.query_selector_all(
                    '[class*="movie"] a, [class*="film"] a, '
                    '[class*="Movie"] a, [class*="Film"] a'
                )

            # Strategy 3: Broader search - any link that looks like a movie page
            if not movie_links:
                all_links = await page.query_selector_all('a[href]')
                movie_links = []
                for link in all_links:
                    href = await link.get_attribute("href") or ""
                    # Exclude common non-movie paths
                    if any(skip in href.lower() for skip in [
                        '/contact', '/about', '/faq', '/terms', '/privacy',
                        '/branch', '/snif', '#', 'javascript:', 'mailto:',
                        '/login', '/register', '/cart',
                    ]):
                        continue
                    # Look for internal paths that could be movie pages
                    if href.startswith("/") and len(href) > 3 and href.count("/") <= 2:
                        movie_links.append(link)

            logger.info(f"[Movieland] Found {len(movie_links)} potential movie links")

            for link in movie_links:
                try:
                    href = await link.get_attribute("href") or ""
                    if not href:
                        continue

                    if href.startswith("/"):
                        full_url = f"{BASE_URL}{href}"
                    elif href.startswith("http"):
                        full_url = href
                    else:
                        continue

                    # Try to get title
                    title = ""
                    title_el = await link.query_selector(
                        'h2, h3, h4, [class*="title"], [class*="name"], '
                        '[class*="Title"], [class*="Name"]'
                    )
                    if title_el:
                        title = (await title_el.inner_text()).strip()
                    if not title:
                        title = (await link.inner_text()).strip()
                    if not title or len(title) < 2:
                        continue

                    # Skip duplicates and non-movie items
                    if title in seen_titles:
                        continue
                    # Skip branch names
                    if any(b["city_he"] in title for b in MOVIELAND_BRANCHES.values()):
                        continue
                    seen_titles.add(title)

                    # Try to get poster
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

            logger.info(f"[Movieland] Scraped {len(movies)} movies from homepage")

            # Visit "now showing" or "movies" page if exists
            for movies_path in ["/now-showing", "/movies", "/סרטים", "/nowshowing"]:
                try:
                    await self._open_url(page, f"{BASE_URL}{movies_path}",
                                         wait_for_network=True)
                    page_movie_links = await page.query_selector_all(
                        'a[href*="/movie/"], a[href*="/seret/"], a[href*="/film/"], '
                        '[class*="movie"] a, [class*="film"] a'
                    )
                    for link in page_movie_links:
                        try:
                            href = await link.get_attribute("href") or ""
                            if not href:
                                continue
                            full_url = f"{BASE_URL}{href}" if href.startswith("/") else href

                            title = ""
                            title_el = await link.query_selector('h2, h3, h4, [class*="title"]')
                            if title_el:
                                title = (await title_el.inner_text()).strip()
                            if not title:
                                title = (await link.inner_text()).strip()
                            if not title or len(title) < 2 or title in seen_titles:
                                continue
                            seen_titles.add(title)

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
                    if page_movie_links:
                        logger.info(f"[Movieland] Found additional movies from {movies_path}")
                        break  # found a working movies page
                except Exception:
                    continue

            if on_progress:
                on_progress("סריקת סרטים - מובילנד", 1, 1, f"נמצאו {len(movies)} סרטים")

        except Exception as e:
            logger.error(f"[Movieland] scrape_movies failed: {e}")
        finally:
            await browser.close()
            await pw.stop()

        return movies

    # ── Screening discovery ──────────────────────────────────────────────

    async def scrape_screenings(self, on_progress=None) -> list[ScrapedScreening]:
        """Scrape screening schedule from movie pages on movieland.co.il."""
        pw, browser, context = await self._launch_browser()
        all_screenings: list[ScrapedScreening] = []

        try:
            page = await context.new_page()

            # First get movie list
            if on_progress:
                on_progress("סריקת הקרנות - מובילנד", 0, 1, "אוסף סרטים")

            movies = await self.scrape_movies(on_progress=None)
            if not movies:
                logger.warning("[Movieland] No movies found, cannot scrape screenings")
                return all_screenings

            # Also capture API responses while browsing
            api_responses: list[dict] = []

            async def capture_response(response):
                url = response.url
                # Look for API endpoints that return screening data
                if any(kw in url.lower() for kw in [
                    'screening', 'showtime', 'schedule', 'event',
                    'biggerpicture', 'session',
                ]):
                    try:
                        ct = response.headers.get("content-type", "")
                        if "json" in ct or "javascript" in ct:
                            body = await response.json()
                            api_responses.append({"url": url, "data": body})
                            logger.info(f"[Movieland] Captured API: {url[:100]}")
                    except Exception:
                        pass

            page.on("response", capture_response)

            for idx, movie in enumerate(movies):
                if not movie.detail_url:
                    continue

                if on_progress:
                    on_progress("סריקת הקרנות - מובילנד", idx + 1, len(movies),
                                movie.title_he or movie.title)

                try:
                    await self._open_url(page, movie.detail_url, wait_for_network=True)
                    await asyncio.sleep(2)

                    # Scroll to load showtimes
                    await page.evaluate("window.scrollBy(0, 600)")
                    await asyncio.sleep(1)

                    # Look for booking links (ecom.biggerpicture.ai)
                    booking_links = await page.query_selector_all(
                        f'a[href*="{BOOKING_DOMAIN}"], '
                        f'a[href*="biggerpicture"], '
                        f'[onclick*="{BOOKING_DOMAIN}"], '
                        f'[data-url*="{BOOKING_DOMAIN}"]'
                    )

                    # Also look for showtime elements that might have data attributes
                    showtime_els = await page.query_selector_all(
                        '[class*="showtime"], [class*="time"], [class*="session"], '
                        '[class*="Showtime"], [class*="Time"], [class*="Session"], '
                        'button[class*="hour"], a[class*="hour"]'
                    )

                    logger.info(
                        f"[Movieland] '{movie.title}': {len(booking_links)} booking links, "
                        f"{len(showtime_els)} showtime elements"
                    )

                    # Extract booking URLs and showtimes
                    found_urls: set[str] = set()

                    for link in booking_links:
                        try:
                            href = (
                                await link.get_attribute("href")
                                or await link.get_attribute("data-url")
                                or ""
                            )
                            if not href:
                                onclick = await link.get_attribute("onclick") or ""
                                match = re.search(r'https?://[^"\']+biggerpicture[^"\']*', onclick)
                                if match:
                                    href = match.group(0)

                            if not href or BOOKING_DOMAIN not in href:
                                continue
                            if href in found_urls:
                                continue
                            found_urls.add(href)

                            # Try to extract context (time, branch) from nearby elements
                            link_text = (await link.inner_text()).strip()
                            parent = await link.evaluate_handle("el => el.parentElement")
                            parent_text = ""
                            try:
                                parent_text = await parent.evaluate("el => el.textContent")
                                parent_text = parent_text.strip()[:200]
                            except Exception:
                                pass

                            # Try to find time pattern
                            time_match = re.search(r'(\d{1,2}:\d{2})', link_text or parent_text)
                            time_str = time_match.group(1) if time_match else ""

                            # Try to find branch/cinema name
                            cinema_name = "Movieland"
                            city = ""
                            for binfo in MOVIELAND_BRANCHES.values():
                                if binfo["city_he"] in parent_text or binfo["name_he"] in parent_text:
                                    cinema_name = binfo["name"]
                                    city = binfo["city"]
                                    break

                            # Parse showtime
                            showtime = None
                            if time_str:
                                try:
                                    today = datetime.now().date()
                                    h, m = map(int, time_str.split(":"))
                                    showtime = datetime.combine(today, datetime.min.time().replace(hour=h, minute=m))
                                except Exception:
                                    pass

                            if showtime:
                                all_screenings.append(ScrapedScreening(
                                    movie_title=movie.title,
                                    cinema_name=cinema_name,
                                    city=city,
                                    showtime=showtime,
                                    hall="",
                                    format="2D",
                                    language="",
                                ))

                        except Exception as e:
                            logger.debug(f"[Movieland] Booking link extraction error: {e}")
                            continue

                    # If no booking links found, try to extract from showtime elements
                    if not found_urls and showtime_els:
                        for el in showtime_els:
                            try:
                                text = (await el.inner_text()).strip()
                                time_match = re.search(r'(\d{1,2}:\d{2})', text)
                                if not time_match:
                                    continue

                                time_str = time_match.group(1)
                                today = datetime.now().date()
                                h, m = map(int, time_str.split(":"))
                                showtime = datetime.combine(
                                    today, datetime.min.time().replace(hour=h, minute=m)
                                )

                                # Try clicking to discover booking URL
                                booking_url = ""
                                try:
                                    async with page.expect_popup(timeout=5000) as popup_info:
                                        await el.click()
                                    popup = await popup_info.value
                                    if BOOKING_DOMAIN in popup.url:
                                        booking_url = popup.url
                                    await popup.close()
                                except Exception:
                                    # Check if navigation happened
                                    if BOOKING_DOMAIN in page.url:
                                        booking_url = page.url
                                        await page.go_back()
                                        await asyncio.sleep(1)

                                all_screenings.append(ScrapedScreening(
                                    movie_title=movie.title,
                                    cinema_name="Movieland",
                                    city="",
                                    showtime=showtime,
                                ))

                            except Exception:
                                continue

                except Exception as e:
                    logger.warning(f"[Movieland] Movie page failed for '{movie.title}': {e}")
                    continue

                await self._human_delay(0.3, 0.8)

            page.remove_listener("response", capture_response)

            # Process any captured API responses
            if api_responses:
                logger.info(f"[Movieland] Captured {len(api_responses)} API responses")
                for resp in api_responses[:5]:
                    logger.warning(f"[Movieland] API URL: {resp['url'][:150]}")
                    data = resp.get("data")
                    if isinstance(data, dict):
                        logger.warning(f"[Movieland] API keys: {list(data.keys())[:10]}")
                    elif isinstance(data, list) and data:
                        logger.warning(f"[Movieland] API list len={len(data)}, "
                                       f"first keys: {list(data[0].keys()) if isinstance(data[0], dict) else 'not dict'}")

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
                on_progress("סורק כיסאות - מובילנד", 0, 1, "אוסף הקרנות")

            # Collect screenings with booking URLs
            movies = await self.scrape_movies(on_progress=None)
            if not movies:
                logger.warning("[Movieland] No movies found")
                return results

            # Collect all booking URLs from movie pages
            booking_items: list[dict] = []  # {movie_title, booking_url, cinema_name, city, showtime, ...}

            for movie in movies:
                if not movie.detail_url:
                    continue
                try:
                    await self._open_url(page, movie.detail_url, wait_for_network=True)
                    await asyncio.sleep(2)
                    await page.evaluate("window.scrollBy(0, 600)")
                    await asyncio.sleep(1)

                    # Find all booking links
                    links = await page.query_selector_all(
                        f'a[href*="{BOOKING_DOMAIN}"]'
                    )

                    seen_urls: set[str] = set()
                    for link in links:
                        try:
                            href = await link.get_attribute("href") or ""
                            if not href or href in seen_urls:
                                continue
                            seen_urls.add(href)

                            # Get context
                            link_text = (await link.inner_text()).strip()
                            parent = await link.evaluate_handle("el => el.parentElement")
                            parent_text = ""
                            try:
                                parent_text = await parent.evaluate("el => el.textContent")
                                parent_text = parent_text.strip()[:300]
                            except Exception:
                                pass

                            # Extract time
                            time_match = re.search(r'(\d{1,2}:\d{2})', link_text or parent_text)
                            time_str = time_match.group(1) if time_match else ""

                            # Extract branch
                            cinema_name = "Movieland"
                            city = ""
                            for binfo in MOVIELAND_BRANCHES.values():
                                if binfo["city_he"] in parent_text or binfo["name_he"] in parent_text:
                                    cinema_name = binfo["name"]
                                    city = binfo["city"]
                                    break

                            # Parse showtime
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

                            # Try to extract hall from parent text
                            hall = ""
                            hall_match = re.search(r'אולם\s*(\d+|[A-Za-z]+)', parent_text)
                            if hall_match:
                                hall = hall_match.group(1)

                            # Try to extract format
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

                            booking_items.append({
                                "movie_title": movie.title,
                                "booking_url": href,
                                "cinema_name": cinema_name,
                                "city": city,
                                "showtime": showtime,
                                "hall": hall,
                                "format": fmt,
                            })

                        except Exception:
                            continue

                except Exception as e:
                    logger.debug(f"[Movieland] Booking URL collection failed for {movie.title}: {e}")
                    continue

                await self._human_delay(0.3, 0.8)

            logger.info(f"[Movieland] Collected {len(booking_items)} booking URLs")

            if on_progress:
                on_progress("סורק כיסאות - מובילנד", 0, len(booking_items), "מתחיל סריקת כיסאות")

            # Navigate to each booking URL and count seats
            for idx, item in enumerate(booking_items):
                if on_progress:
                    on_progress("סורק כיסאות - מובילנד", idx + 1, len(booking_items),
                                item["movie_title"])

                try:
                    # Navigate directly to seat map (BiggerPicture opens it directly!)
                    await self._open_url(page, item["booking_url"], wait_for_network=True)
                    await asyncio.sleep(3)  # extra wait for SPA

                    total, sold, sold_positions = await self._count_seats_on_page(
                        page,
                        movie_title=item["movie_title"],
                        screening_time=str(item.get("showtime", "")),
                    )

                    screening = ScrapedScreening(
                        movie_title=item["movie_title"],
                        cinema_name=item["cinema_name"],
                        city=item["city"],
                        showtime=item["showtime"] or datetime.now(),
                        hall=item["hall"],
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
