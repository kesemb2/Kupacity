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

        Movieland branch pages follow the pattern:
        https://movieland.co.il/theater/{theater_id}/{city_he}

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

        # Strategy 1: Find /theater/ links anywhere on the page
        # These follow the pattern /theater/{id}/{city_he}
        all_links = await page.query_selector_all('a[href]')
        theater_links: list[tuple[str, str]] = []  # (href, text)

        for link in all_links:
            try:
                href = await link.get_attribute("href") or ""
                if self._is_junk_link(href):
                    continue
                text = (await link.inner_text()).strip()

                # Collect /theater/ links specifically
                if "/theater/" in href or "/theater/" in href.lower():
                    if href.startswith("/"):
                        href = f"{BASE_URL}{href}"
                    theater_links.append((href, text))

                # Also try to match branches by city name in any link
                for bid, binfo in MOVIELAND_BRANCHES.items():
                    if bid in branch_urls:
                        continue
                    city_he = binfo["city_he"]
                    # Match by URL containing city name or by link text
                    if city_he in href or city_he in text:
                        if self._is_junk_link(href):
                            continue
                        if href.startswith("/"):
                            href = f"{BASE_URL}{href}"
                        if href.startswith("http") and "movieland.co.il" in href:
                            branch_urls[bid] = href
                            logger.info(f"[Movieland] Found branch URL: {binfo['name']} -> {href}")

            except Exception:
                continue

        # Process collected /theater/ links
        for href, text in theater_links:
            for bid, binfo in MOVIELAND_BRANCHES.items():
                if bid in branch_urls:
                    continue
                # Check if this theater link matches a branch
                city_he = binfo["city_he"]
                if city_he in href or city_he in text:
                    branch_urls[bid] = href
                    logger.info(f"[Movieland] Found branch URL via /theater/: {binfo['name']} -> {href}")

        logger.info(f"[Movieland] Strategy 1 (page links): found {len(branch_urls)} branches")

        # Strategy 2: Hover over "סניפים" in the nav to reveal dropdown
        if len(branch_urls) < len(MOVIELAND_BRANCHES):
            try:
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
                    # Try hover
                    await snifim_el.hover()
                    await asyncio.sleep(1.5)
                    logger.info("[Movieland] Hovered on 'סניפים' menu item")

                    try:
                        await page.screenshot(
                            path=_debug_screenshot_path("dropdown", branch="snifim")
                        )
                    except Exception:
                        pass

                    # Also try clicking it
                    try:
                        await snifim_el.click()
                        await asyncio.sleep(2)
                    except Exception:
                        pass

                    # Scan for new links
                    dropdown_links = await page.query_selector_all('a[href]')
                    for link in dropdown_links:
                        try:
                            href = await link.get_attribute("href") or ""
                            if self._is_junk_link(href):
                                continue
                            text = (await link.inner_text()).strip()

                            for bid, binfo in MOVIELAND_BRANCHES.items():
                                if bid in branch_urls:
                                    continue
                                city_he = binfo["city_he"]
                                if city_he in href or city_he in text:
                                    if href.startswith("/"):
                                        href = f"{BASE_URL}{href}"
                                    if href.startswith("http") and "movieland.co.il" in href:
                                        branch_urls[bid] = href
                                        logger.info(f"[Movieland] Found branch URL via dropdown: {binfo['name']} -> {href}")
                        except Exception:
                            continue

                    # Navigate back to homepage if we left it
                    if page.url != BASE_URL and page.url != f"{BASE_URL}/":
                        await self._open_url(page, BASE_URL, wait_for_network=True)

            except Exception as e:
                logger.warning(f"[Movieland] Dropdown discovery failed: {e}")

        logger.info(f"[Movieland] Strategy 2 (dropdown): total {len(branch_urls)} branches")

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

        # ── Analyze page structure via JS for movie section detection ──
        page_structure = await page.evaluate("""() => {
            const result = {
                orderLinks: [],
                movieContainers: [],
                headings: [],
                allClassNames: new Set(),
            };

            // Find all /order/ links and their context
            const links = document.querySelectorAll('a[href]');
            for (const a of links) {
                const href = a.getAttribute('href') || '';
                if (href.includes('/order/') && href.includes('eventID')) {
                    // Walk up to find movie context
                    let movieTitle = '';
                    let containerClasses = [];
                    let parentInfo = [];
                    let p = a.parentElement;
                    for (let i = 0; i < 8 && p; i++) {
                        const cls = p.getAttribute('class') || '';
                        const tag = p.tagName;
                        parentInfo.push(tag + (cls ? '.' + cls.replace(/\\s+/g, '.') : ''));
                        if (cls) containerClasses.push(cls);

                        // Check for headings inside this parent
                        const headings = p.querySelectorAll('h1, h2, h3, h4, h5, h6, [class*="title"], [class*="Title"], [class*="name"], [class*="Name"]');
                        for (const h of headings) {
                            const txt = h.textContent.trim();
                            if (txt.length > 2 && txt.length < 80 && !movieTitle) {
                                movieTitle = txt;
                            }
                        }
                        p = p.parentElement;
                    }

                    result.orderLinks.push({
                        href: href,
                        text: a.textContent.trim().substring(0, 50),
                        movieTitle: movieTitle,
                        parents: parentInfo,
                        containerClasses: containerClasses,
                    });
                }
            }

            // Collect all unique class names on the page for analysis
            const allEls = document.querySelectorAll('*');
            const classCounts = {};
            for (const el of allEls) {
                const cls = el.getAttribute('class') || '';
                if (cls) {
                    for (const c of cls.split(/\\s+/)) {
                        if (c.length > 2) {
                            classCounts[c] = (classCounts[c] || 0) + 1;
                        }
                    }
                }
            }

            // Find classes that appear multiple times (likely movie containers)
            const repeatedClasses = {};
            for (const [cls, count] of Object.entries(classCounts)) {
                if (count >= 3 && count <= 50) {
                    repeatedClasses[cls] = count;
                }
            }

            // Collect all headings
            const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
            for (const h of headings) {
                result.headings.push({
                    tag: h.tagName,
                    text: h.textContent.trim().substring(0, 80),
                    class: h.getAttribute('class') || '',
                    parentClass: h.parentElement ? (h.parentElement.getAttribute('class') || '') : '',
                });
            }

            return {
                orderLinksCount: result.orderLinks.length,
                orderLinks: result.orderLinks.slice(0, 20),
                headings: result.headings.slice(0, 30),
                repeatedClasses: repeatedClasses,
            };
        }""")

        if page_structure:
            logger.info(
                f"[Movieland] Page structure analysis for {binfo['name']}: "
                f"{page_structure.get('orderLinksCount', 0)} order links, "
                f"{len(page_structure.get('headings', []))} headings"
            )
            if page_structure.get("orderLinks"):
                for ol in page_structure["orderLinks"][:5]:
                    logger.info(
                        f"[Movieland]   Order link: text='{ol.get('text', '')}' "
                        f"movie='{ol.get('movieTitle', '')}' "
                        f"parents={ol.get('parents', [])[:3]}"
                    )
            if page_structure.get("repeatedClasses"):
                logger.info(f"[Movieland]   Repeated classes: {page_structure['repeatedClasses']}")

        # ── Collect movies via smart order-link parent traversal ───────
        # Instead of guessing CSS selectors, find all /order/ links and
        # walk up the DOM to find their movie container + title
        order_links_data = page_structure.get("orderLinks", []) if page_structure else []

        # Group order links by movie title
        movie_groups: dict[str, list[dict]] = {}
        for ol_data in order_links_data:
            title = ol_data.get("movieTitle", "").strip()
            if not title:
                title = ""
            if title not in movie_groups:
                movie_groups[title] = []
            movie_groups[title].append(ol_data)

        # Also get all links from the page for fallback
        all_page_links = await page.query_selector_all('a[href]')

        # Now use the JS-discovered structure to find movie containers
        # Try to determine the common container class from the order links
        container_class = ""
        if order_links_data:
            # Find the most common parent class that appears across multiple order links
            parent_class_counts: dict[str, int] = {}
            for ol_data in order_links_data:
                for cls_str in ol_data.get("containerClasses", []):
                    for cls in cls_str.split():
                        if len(cls) > 2:
                            parent_class_counts[cls] = parent_class_counts.get(cls, 0) + 1
            # Pick class that appears most (likely the movie container class)
            if parent_class_counts:
                container_class = max(parent_class_counts, key=parent_class_counts.get)
                logger.info(f"[Movieland] Detected container class: '{container_class}' "
                            f"(appears {parent_class_counts[container_class]} times)")

        # Strategy 1: Use JS-extracted movie titles from order link parents
        if movie_groups:
            for title, links_data in movie_groups.items():
                if not title or len(title) < 2:
                    continue
                if title in seen_titles:
                    continue

                # Skip navigation/junk titles
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
                movies.append(ScrapedMovie(
                    title=title, title_he=title,
                    detail_url=branch_url,
                ))

                # Extract showtimes from links
                for ol_data in links_data:
                    link_text = ol_data.get("text", "")
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

                    # Extract hall/format from link text or parents
                    hall = ""
                    fmt = "2D"
                    context_text = " ".join(ol_data.get("parents", []))
                    hall_match = re.search(r'אולם\s*(\d+|[A-Za-z]+)', context_text + " " + link_text)
                    if hall_match:
                        hall = hall_match.group(1)
                    upper = (context_text + " " + link_text).upper()
                    if "IMAX" in upper:
                        fmt = "IMAX"
                    elif "4DX" in upper:
                        fmt = "4DX"
                    elif "3D" in upper:
                        fmt = "3D"
                    elif "VIP" in upper:
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
                        href = ol_data.get("href", "")
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

            logger.info(f"[Movieland] Strategy 1 (JS order-link parents): {len(movies)} movies, {len(screenings)} screenings")

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

        # Take pre-annotation debug screenshot
        try:
            await page.screenshot(path=_debug_screenshot_path(
                "step4_seats", movie=movie_title, branch=branch_he,
                time_str=screening_time,
            ))
        except Exception:
            pass

        # ── Annotate seats for visual debugging ──────────────────────
        # Add colored borders: green=available, red=sold, orange=unknown
        try:
            await page.evaluate("""() => {
                const seen = new Set();

                // Find all mvl-seat image elements
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

                    const isSold = src.includes('/unavailable/');
                    const isAvail = src.includes('/available/');
                    const borderColor = isSold ? '#ff0000' : (isAvail ? '#00ff00' : '#ff8800');

                    const div = document.createElement('div');
                    div.style.cssText = `position:fixed;left:${bbox.left}px;top:${bbox.top}px;`
                        + `width:${bbox.width}px;height:${bbox.height}px;`
                        + `border:2px solid ${borderColor};`
                        + `pointer-events:none;z-index:99999;box-sizing:border-box;`;
                    div.className = '_mvl_seat_debug_overlay';
                    document.body.appendChild(div);
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

                    const isSold = bgImg.includes('/unavailable/');
                    const borderColor = isSold ? '#ff0000' : '#00ff00';

                    const div = document.createElement('div');
                    div.style.cssText = `position:fixed;left:${bbox.left}px;top:${bbox.top}px;`
                        + `width:${bbox.width}px;height:${bbox.height}px;`
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

        Movieland uses two types of booking URLs:
        1. /order/?eventID=X&theaterId=Y → redirects to ecom.biggerpicture.ai seat map
        2. Direct ecom.biggerpicture.ai links (rare)

        Both end up on the BiggerPicture seat map page where we count seats.
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
                    booking_url = item["booking_url"]
                    is_order_url = "/order/" in booking_url and "eventID" in booking_url

                    if is_order_url:
                        # /order/ URLs redirect to ecom.biggerpicture.ai
                        # Navigate and wait for redirect to complete
                        logger.info(f"[Movieland] Navigating to order URL: {booking_url}")
                        try:
                            await page.goto(booking_url, wait_until="domcontentloaded", timeout=30000)
                        except Exception as e:
                            logger.warning(f"[Movieland] Order URL navigation: {e}")

                        # Step 3: screenshot after order URL load (before redirect)
                        try:
                            await page.screenshot(path=_debug_screenshot_path(
                                "step3_order",
                                movie=item["movie_title"],
                                branch=item.get("branch_he", ""),
                                time_str=item.get("time_str", ""),
                            ))
                        except Exception:
                            pass

                        # Wait for redirect to biggerpicture
                        for _ in range(10):
                            if BOOKING_DOMAIN in page.url:
                                break
                            await asyncio.sleep(1)

                        if BOOKING_DOMAIN not in page.url:
                            logger.warning(
                                f"[Movieland] Order URL did not redirect to {BOOKING_DOMAIN}. "
                                f"Current URL: {page.url}"
                            )
                            # Take a debug screenshot to see what happened
                            try:
                                await page.screenshot(path=_debug_screenshot_path(
                                    "redirect_fail",
                                    movie=item["movie_title"],
                                    branch=item.get("branch_he", ""),
                                ))
                            except Exception:
                                pass
                            continue

                        logger.info(f"[Movieland] Redirected to: {page.url}")
                        await asyncio.sleep(3)  # Wait for SPA to render

                        # Step 3b: screenshot after successful redirect
                        try:
                            await page.screenshot(path=_debug_screenshot_path(
                                "step3_redirect",
                                movie=item["movie_title"],
                                branch=item.get("branch_he", ""),
                                time_str=item.get("time_str", ""),
                            ))
                        except Exception:
                            pass
                    else:
                        # Direct biggerpicture URL
                        await self._open_url(page, booking_url, wait_for_network=True)
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
