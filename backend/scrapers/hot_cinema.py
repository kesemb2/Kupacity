"""
Scraper for Hot Cinema chain using Playwright.

Playwright is used with stealth-like settings to bypass basic bot detection.
The bundled Chromium binary is auto-detected so no external Chrome install
is needed.

URL patterns:
- Theater page:  https://hotcinema.co.il/theater/{id}/{slug}
- Movie page:    https://hotcinema.co.il/movie/{id}/{slug}
- Tickets:       https://tickets.hotcinema.co.il/site/{id}

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
_TICKET_DEBUG_SCREENSHOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "debug_tickets.png")
_DEBUG_SCREENSHOTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "debug_screenshots")


def _debug_screenshot_path(step: str, movie_title: str = "", screening_time: str = "",
                           branch: str = "") -> str:
    """Generate a unique debug screenshot path.

    Format: hot_{branch}_{movie}_{time}_{step}_{timestamp}.png
    """
    os.makedirs(_DEBUG_SCREENSHOTS_DIR, exist_ok=True)
    ts = datetime.now().strftime("%H%M%S")
    safe = lambda s: re.sub(r'[^\w\u0590-\u05FF -]', '', s)[:20].strip().replace(' ', '_')
    safe_time = screening_time.replace(':', '').replace(' ', '_')[:10]
    parts = ["hot"]
    if branch:
        parts.append(safe(branch))
    if movie_title:
        parts.append(safe(movie_title))
    if safe_time:
        parts.append(safe_time)
    parts.append(step)
    parts.append(ts)
    filename = "_".join(p for p in parts if p) + ".png"
    return os.path.join(_DEBUG_SCREENSHOTS_DIR, filename)

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


def _extract_movie_id(url: str) -> str | None:
    """Extract numeric movie ID from a Hot Cinema movie URL like /movie/3571/slug."""
    m = re.search(r'/movie/(\d+)', url)
    return m.group(1) if m else None


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

    async def _count_seats_on_page(self, page: Page,
                                    movie_title: str = "", screening_time: str = "",
                                    branch: str = "") -> tuple[int, int, list]:
        total = 0
        sold = 0
        sold_positions = []

        # Wait a bit for seat map to render (Angular SPA)
        await asyncio.sleep(2)

        # Run BOTH detection methods in parallel inside one page.evaluate
        seat_data = await page.evaluate("""() => {
            // ===== UTILITY FUNCTIONS =====
            function parseColor(c) {
                if (!c || c === 'none' || c === 'transparent' || c === 'rgba(0, 0, 0, 0)') return null;
                let m = c.match(/rgba?\\((\\d+),\\s*(\\d+),\\s*(\\d+)/);
                if (m) return {r: parseInt(m[1]), g: parseInt(m[2]), b: parseInt(m[3])};
                m = c.match(/^#([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})/i);
                if (m) return {r: parseInt(m[1],16), g: parseInt(m[2],16), b: parseInt(m[3],16)};
                return null;
            }

            function isGreenish(c) {
                return c && c.g > 80 && c.g > c.r * 1.2 && c.g > c.b * 1.2;
            }

            function isSignificantColor(c) {
                // Not transparent/black/white - i.e. a real visible color
                if (!c) return false;
                const sum = c.r + c.g + c.b;
                return sum > 60 && sum < 700 && (c.r > 30 || c.g > 30 || c.b > 30);
            }

            function colorToStr(c) {
                return c ? `rgb(${c.r},${c.g},${c.b})` : 'none';
            }

            // ===== FIND SEAT CONTAINER =====
            const seatPlan = document.querySelector('.seat-plan--inner-view')
                          || document.querySelector('.seat-plan--inner')
                          || document.querySelector('.seat-plan');

            if (!seatPlan) {
                return {total: 0, sold: 0, error: 'no_seat_plan_container',
                        containerHTML: document.body ? document.body.innerHTML.substring(0, 3000) : ''};
            }

            const allEls = seatPlan.querySelectorAll('*');

            // ===== METHOD A: COLOR-BASED DETECTION =====
            const colorCandidates = [];
            const seatSizedAll = []; // ALL seat-sized elements (for debug)
            const allUniqueColors = {};
            const tagCounts = {};

            for (const el of allEls) {
                const tag = el.tagName;
                tagCounts[tag] = (tagCounts[tag] || 0) + 1;

                const bbox = el.getBoundingClientRect();
                if (bbox.width < 10 || bbox.width > 70 || bbox.height < 10 || bbox.height > 70)
                    continue;
                const aspect = bbox.width / bbox.height;
                if (aspect < 0.4 || aspect > 2.5)
                    continue;

                // Skip text-only elements (row numbers like "10", "11", "12")
                const txt = el.textContent ? el.textContent.trim() : '';
                if (txt && /^\\d{1,2}$/.test(txt) && el.children.length === 0) continue;

                // This is a seat-sized element
                const style = window.getComputedStyle(el);
                const cls = (typeof el.className === 'string' ? el.className
                    : el.className && el.className.baseVal ? el.className.baseVal
                    : el.getAttribute('class') || '').toLowerCase();
                const bgRaw = style.backgroundColor;
                const fillRaw = style.fill;

                // Track ALL unique colors for debug
                const bgParsed = parseColor(bgRaw);
                if (bgParsed) {
                    const key = `bg:${colorToStr(bgParsed)}`;
                    allUniqueColors[key] = (allUniqueColors[key] || 0) + 1;
                }
                const fillParsed = parseColor(fillRaw);
                if (fillParsed) {
                    const key = `fill:${colorToStr(fillParsed)}`;
                    allUniqueColors[key] = (allUniqueColors[key] || 0) + 1;
                }

                // Save ALL seat-sized for debug (limited)
                if (seatSizedAll.length < 50) {
                    const parentEl = el.parentElement;
                    seatSizedAll.push({
                        tag, cls: cls.substring(0, 60),
                        w: Math.round(bbox.width), h: Math.round(bbox.height),
                        x: Math.round(bbox.left), y: Math.round(bbox.top),
                        bg: bgRaw ? bgRaw.substring(0, 40) : 'none',
                        fill: fillRaw ? fillRaw.substring(0, 40) : 'none',
                        fillAttr: (el.getAttribute('fill') || '').substring(0, 30),
                        bgImg: (style.backgroundImage || '').substring(0, 80),
                        svgHref: (el.getAttribute('href') || '').substring(0, 60),
                        text: txt.substring(0, 10),
                        filter: (style.filter || '').substring(0, 30),
                        opacity: style.opacity,
                        parentBg: parentEl ? (window.getComputedStyle(parentEl).backgroundColor || '').substring(0, 40) : 'none',
                        parentCls: parentEl ? (parentEl.className || '').toString().substring(0, 40) : '',
                    });
                }

                // Check color sources - accept ANY significant color (not just green/gray)
                let color = null;
                let colorSource = '';

                // 0. Check SVG <image> href or CSS background-image for seat status
                const bgImg = style.backgroundImage || '';
                const svgHref = el.getAttribute('href')
                    || el.getAttributeNS('http://www.w3.org/1999/xlink', 'href') || '';
                const seatImgUrl = bgImg + ' ' + svgHref;
                if (seatImgUrl.includes('/unavailable/')) {
                    color = {r: 115, g: 115, b: 116}; // gray = sold
                    colorSource = 'seat-img-unavailable';
                } else if (seatImgUrl.includes('/available/')) {
                    color = {r: 76, g: 175, b: 80}; // green = available
                    colorSource = 'seat-img-available';
                }

                const bg = parseColor(bgRaw);
                if (!color && bg && isSignificantColor(bg)) {
                    color = bg;
                    colorSource = 'bg:' + bgRaw;
                }
                if (!color) {
                    const fill = parseColor(fillRaw);
                    if (fill && isSignificantColor(fill)) {
                        color = fill;
                        colorSource = 'fill:' + fillRaw;
                    }
                }
                if (!color) {
                    const fillAttr = parseColor(el.getAttribute('fill'));
                    if (fillAttr && isSignificantColor(fillAttr)) {
                        color = fillAttr;
                        colorSource = 'attr:' + el.getAttribute('fill');
                    }
                }

                // 4. Check CSS filter for grayscale (seat might be green + grayscale filter = looks gray)
                if (!color) {
                    const filter = style.filter || '';
                    if (filter.includes('grayscale') || filter.includes('saturate(0)')) {
                        const bg2 = parseColor(bgRaw);
                        if (bg2) {
                            color = {r: 115, g: 115, b: 116}; // treat as gray/sold
                            colorSource = 'filter-grayscale';
                        }
                    }
                }

                // 5. Check parent element backgroundColor
                if (!color) {
                    const parent = el.parentElement;
                    if (parent) {
                        const parentStyle = window.getComputedStyle(parent);
                        const parentBg = parseColor(parentStyle.backgroundColor);
                        if (parentBg && isSignificantColor(parentBg)) {
                            color = parentBg;
                            colorSource = 'parent-bg:' + parentStyle.backgroundColor;
                        }
                    }
                }

                const hasAvailableClass = cls.includes('available') || cls.includes('free') || cls.includes('open');
                const hasSoldClass = cls.includes('sold') || cls.includes('occupied') || cls.includes('taken')
                    || cls.includes('reserved') || cls.includes('booked') || cls.includes('unavailable');

                if (!color && !hasAvailableClass && !hasSoldClass) continue;

                // Key logic: class/href > color. SVG fill is green for ALL seats (inherited)
                const green = hasSoldClass ? false
                    : hasAvailableClass ? true
                    : (color ? isGreenish(color) : false);

                colorCandidates.push({
                    rect: {left: bbox.left, top: bbox.top, width: bbox.width, height: bbox.height},
                    color, colorSource,
                    isGreen: green,
                    isGray: !green,  // anything not green is sold
                    tag, cls: cls.substring(0, 40),
                });
            }

            // Deduplicate color candidates
            const beforeDedup = colorCandidates.length;
            const seen = new Set();
            const dedupedColor = [];
            for (const c of colorCandidates) {
                const key = Math.round(c.rect.left / 5) + ',' + Math.round(c.rect.top / 5);
                if (!seen.has(key)) {
                    seen.add(key);
                    dedupedColor.push(c);
                }
            }

            // Legend exclusion
            const ys = dedupedColor.map(c => c.rect.top).sort((a, b) => a - b);
            let maxGap = 0, gapY = Infinity;
            for (let i = 1; i < ys.length; i++) {
                const gap = ys[i] - ys[i - 1];
                if (gap > maxGap) { maxGap = gap; gapY = ys[i]; }
            }
            const cutoffY = maxGap > 150 ? gapY : Infinity;

            let colorTotal = 0, colorSold = 0;
            let cutByLegend = 0;
            const colorSamples = {};
            const colorSampleSeats = [];
            const colorExcludedSample = [];
            const colorSoldPositions = [];

            for (const c of dedupedColor) {
                if (c.rect.top >= cutoffY) {
                    cutByLegend++;
                    if (colorExcludedSample.length < 5) {
                        colorExcludedSample.push({
                            tag: c.tag, cls: c.cls, y: Math.round(c.rect.top),
                            reason: 'legend_cutoff', colorSource: c.colorSource,
                        });
                    }
                    continue;
                }
                colorTotal++;
                const key = c.colorSource || 'class-only';
                colorSamples[key] = (colorSamples[key] || 0) + 1;
                if (c.isGray) {
                    colorSold++;
                    colorSoldPositions.push([Math.round(c.rect.left / 5), Math.round(c.rect.top / 5)]);
                }
                if (colorSampleSeats.length < 10) {
                    colorSampleSeats.push({
                        tag: c.tag, cls: c.cls,
                        w: Math.round(c.rect.width), h: Math.round(c.rect.height),
                        x: Math.round(c.rect.left), y: Math.round(c.rect.top),
                        colorSource: c.colorSource,
                        isGreen: c.isGreen, isGray: c.isGray,
                    });
                }
            }

            // ===== METHOD B: CLASS/ATTRIBUTE-BASED DETECTION =====
            // Look for elements with "seat" in class/tag and status indicators
            let classTotal = 0, classSold = 0;
            const classSampleSeats = [];
            const classStatusCounts = {};
            const classSoldPositions = [];

            // Search entire document (not just seatPlan) for seat elements
            const allPageEls = document.querySelectorAll('*');
            for (const el of allPageEls) {
                const tag = el.tagName.toLowerCase();
                const cls = (typeof el.className === 'string' ? el.className
                    : el.className && el.className.baseVal ? el.className.baseVal
                    : el.getAttribute('class') || '').toLowerCase();
                const id = (el.id || '').toLowerCase();

                // Must have "seat" in class, tag, or id
                const isSeatElement = cls.includes('seat') || tag.includes('seat') || id.includes('seat');
                if (!isSeatElement) continue;

                // Check for status indicators
                const isAvailable = cls.includes('available') || cls.includes('free') || cls.includes('open')
                    || el.getAttribute('data-status') === 'available'
                    || el.getAttribute('data-available') === 'true'
                    || el.getAttribute('data-seat-status') === 'available';

                const isSold = cls.includes('sold') || cls.includes('occupied') || cls.includes('taken')
                    || cls.includes('reserved') || cls.includes('booked') || cls.includes('unavailable')
                    || cls.includes('disabled')
                    || el.getAttribute('data-status') === 'sold'
                    || el.getAttribute('data-status') === 'occupied'
                    || el.getAttribute('data-available') === 'false'
                    || el.getAttribute('data-seat-status') === 'sold'
                    || el.getAttribute('data-seat-status') === 'occupied';

                if (!isAvailable && !isSold) {
                    // Track non-status seat elements for debug
                    const statusKey = 'unknown:' + cls.substring(0, 30);
                    classStatusCounts[statusKey] = (classStatusCounts[statusKey] || 0) + 1;
                    continue;
                }

                classTotal++;
                if (isSold) {
                    classSold++;
                    const bbox2 = el.getBoundingClientRect();
                    classSoldPositions.push([Math.round(bbox2.left / 5), Math.round(bbox2.top / 5)]);
                }

                const statusKey = isSold ? 'sold' : 'available';
                classStatusCounts[statusKey] = (classStatusCounts[statusKey] || 0) + 1;

                if (classSampleSeats.length < 10) {
                    const bbox = el.getBoundingClientRect();
                    classSampleSeats.push({
                        tag: el.tagName, cls: cls.substring(0, 60),
                        id: id.substring(0, 30),
                        w: Math.round(bbox.width), h: Math.round(bbox.height),
                        x: Math.round(bbox.left), y: Math.round(bbox.top),
                        status: statusKey,
                        dataAttrs: {
                            status: el.getAttribute('data-status'),
                            seatStatus: el.getAttribute('data-seat-status'),
                            available: el.getAttribute('data-available'),
                            seatId: el.getAttribute('data-seat-id'),
                        },
                    });
                }
            }

            // ===== ANNOTATE SEATS FOR SCREENSHOT =====
            // Add colored borders to detected seats so screenshot shows what was found
            const annotations = [];
            for (const c of dedupedColor) {
                const borderColor = c.rect.top >= cutoffY ? '#0088ff' // blue = legend
                    : c.isGreen ? '#00ff00' // green = available
                    : c.isGray ? '#ff0000' // red = sold
                    : '#ff8800'; // orange = unknown
                annotations.push({
                    left: c.rect.left, top: c.rect.top,
                    width: c.rect.width, height: c.rect.height,
                    borderColor,
                });
            }

            // Create overlay divs for annotation
            for (const a of annotations) {
                const div = document.createElement('div');
                div.style.cssText = `position:fixed;left:${a.left}px;top:${a.top}px;`
                    + `width:${a.width}px;height:${a.height}px;`
                    + `border:2px solid ${a.borderColor};`
                    + `pointer-events:none;z-index:99999;box-sizing:border-box;`;
                div.className = '_seat_debug_overlay';
                document.body.appendChild(div);
            }

            // Get seat container innerHTML for debug (truncated)
            const seatContainerHTML = seatPlan.innerHTML.substring(0, 3000);

            return {
                // Method A results (color-based)
                colorMethod: {
                    total: colorTotal, sold: colorSold,
                    seatSizedTotal: seatSizedAll.length,
                    beforeDedup, afterDedup: dedupedColor.length,
                    cutByLegend,
                    cutoffY: cutoffY === Infinity ? 'none' : Math.round(cutoffY),
                    maxGap: Math.round(maxGap),
                    colorSamples,
                    sampleSeats: colorSampleSeats,
                    excludedSample: colorExcludedSample,
                    soldPositions: colorSoldPositions,
                },
                // Method B results (class/attribute-based)
                classMethod: {
                    total: classTotal, sold: classSold,
                    statusCounts: classStatusCounts,
                    sampleSeats: classSampleSeats,
                    soldPositions: classSoldPositions,
                },
                // Debug info
                debug: {
                    allElsCount: allEls.length,
                    tagCounts,
                    allUniqueColors,
                    seatSizedAll,
                    seatContainerHTML,
                    annotationsCount: annotations.length,
                },
            };
        }""")

        # Take annotated screenshot (step 5) - overlays are now on the page
        try:
            await page.screenshot(path=_debug_screenshot_path("step5_annotated", movie_title, screening_time, branch=branch))
            logger.info("[Hot Cinema] Step 5 screenshot saved (annotated seats)")
        except Exception:
            pass

        # Clean up overlay divs
        try:
            await page.evaluate("""() => {
                document.querySelectorAll('._seat_debug_overlay').forEach(el => el.remove());
            }""")
        except Exception:
            pass

        if seat_data:
            color = seat_data.get("colorMethod", {})
            cls = seat_data.get("classMethod", {})
            debug = seat_data.get("debug", {})

            # Log both methods side by side
            logger.warning(
                f"[Hot Cinema] Seats COLOR method: {color.get('total', 0)} total, "
                f"{color.get('sold', 0)} sold | "
                f"seatSized={color.get('seatSizedTotal', '?')}, "
                f"beforeDedup={color.get('beforeDedup', '?')}, "
                f"afterDedup={color.get('afterDedup', '?')}, "
                f"cutByLegend={color.get('cutByLegend', '?')}, "
                f"cutoffY={color.get('cutoffY', '?')}"
            )
            logger.warning(
                f"[Hot Cinema] Seats CLASS method: {cls.get('total', 0)} total, "
                f"{cls.get('sold', 0)} sold | "
                f"statusCounts={cls.get('statusCounts', {})}"
            )
            logger.warning(
                f"[Hot Cinema] All unique colors in seat-sized elements: "
                f"{debug.get('allUniqueColors', {})}"
            )
            logger.warning(
                f"[Hot Cinema] Color method samples: {color.get('sampleSeats', [])[:5]}"
            )
            logger.warning(
                f"[Hot Cinema] Class method samples: {cls.get('sampleSeats', [])[:5]}"
            )
            if color.get('excludedSample'):
                logger.warning(
                    f"[Hot Cinema] Excluded by legend: {color.get('excludedSample')}"
                )
            logger.info(
                f"[Hot Cinema] Seat-sized elements (all): {debug.get('seatSizedAll', [])[:10]}"
            )

            # Use the method with higher total count
            color_total = color.get("total", 0)
            class_total = cls.get("total", 0)

            sold_positions = []
            if color_total >= 10 or class_total >= 10:
                if class_total > color_total:
                    total = class_total
                    sold = cls.get("sold", 0)
                    sold_positions = cls.get("soldPositions", [])
                    logger.info(f"[Hot Cinema] Using CLASS method: {sold}/{total}")
                else:
                    total = color_total
                    sold = color.get("sold", 0)
                    sold_positions = color.get("soldPositions", [])
                    logger.info(f"[Hot Cinema] Using COLOR method: {sold}/{total}")
            else:
                logger.warning(
                    f"[Hot Cinema] Both methods found too few seats. "
                    f"color={color_total}, class={class_total}. "
                    f"seat_data error={seat_data.get('error', 'none')}"
                )
                # Log container HTML for debugging
                html = debug.get("seatContainerHTML", "")
                if html:
                    logger.warning(f"[Hot Cinema] Seat container HTML (first 1000): {html[:1000]}")

        # Fallback: look for text-based seat info
        if total == 0:
            try:
                body_text = await page.inner_text("body")
                remaining_match = re.search(r"נותרו\s+(\d+)\s+מקומות", body_text)
                if remaining_match:
                    total = int(remaining_match.group(1))
                    sold = 0
                ratio_match = re.search(r"(\d+)\s*/\s*(\d+)\s*(?:מקומות|כיסאות|seats)", body_text)
                if ratio_match:
                    s, t = int(ratio_match.group(1)), int(ratio_match.group(2))
                    if t >= s and t >= 10:
                        sold = s
                        total = t
            except Exception:
                pass

        return total, sold, sold_positions

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
                        "booking_url": "",
                    })
                except Exception as e:
                    logger.debug(f"[Hot Cinema] Failed to parse date entry for '{movie_title}': {e}")
                    continue

        logger.info(f"[Hot Cinema] '{movie_title}': {len(parsed)} screenings parsed from API")
        return parsed

    async def _fetch_screenings_api(self, page: Page, movie_id: str,
                                     movie_title: str, days: int = 7) -> list[dict]:
        """Fetch screening data directly from the movieevents API for the next N days.

        Much faster than loading movie pages — uses page.request.get() which
        shares the browser context's cookies and headers.
        """
        MOVIEEVENTS_URL = f"{BASE_URL}/tickets/movieevents"
        all_parsed: list[dict] = []
        today = datetime.now().date()

        for day_offset in range(days):
            target_date = today + timedelta(days=day_offset)
            date_str = target_date.strftime("%d/%m/%Y")
            api_url = f"{MOVIEEVENTS_URL}?movieid={movie_id}&date={date_str}&theatreid=&time=&type=&lang="

            try:
                resp = await page.request.get(api_url)
                if resp.status != 200:
                    logger.debug(f"[Hot Cinema] API {resp.status} for movie {movie_id} date {date_str}")
                    continue

                theaters = await resp.json()
                if not isinstance(theaters, list):
                    continue

                # Log first theater's raw keys to discover URL fields
                if theaters and day_offset == 0:
                    first = theaters[0]
                    logger.warning(f"[Hot Cinema] API theater keys: {list(first.keys())}")
                    if first.get("Dates"):
                        first_date = first["Dates"][0]
                        logger.warning(f"[Hot Cinema] API date entry keys: {list(first_date.keys())}")
                        logger.warning(f"[Hot Cinema] API sample: TheaterID={first.get('TheaterID')}, "
                                       f"EventId={first_date.get('EventId')}, "
                                       f"Url={first_date.get('Url', 'N/A')}, "
                                       f"BookingUrl={first_date.get('BookingUrl', 'N/A')}, "
                                       f"Link={first_date.get('Link', 'N/A')}")

                for theater in theaters:
                    theater_name = theater.get("TheaterName", "")
                    theater_id = theater.get("TheaterID")
                    cinema_name, city = _resolve_cinema(f"HOT CINEMA {theater_name}")

                    # Theater-level format flags
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

                    dubbed_lang = theater.get("DubbedLanguage")
                    subtitled_lang = theater.get("SubtitledLanguage")

                    for date_entry in theater.get("Dates", []):
                        try:
                            raw_date = date_entry.get("FormattedDate") or date_entry.get("Date", "")
                            if not raw_date or len(raw_date) < 16:
                                continue
                            raw_date = raw_date.replace("T", " ")
                            showtime = datetime.strptime(raw_date[:19], "%Y-%m-%d %H:%M:%S")

                            entry_dubbed = date_entry.get("DubbedLanguage") or dubbed_lang
                            entry_subtitled = date_entry.get("SubtitledLanguage") or subtitled_lang
                            language = "dubbed" if entry_dubbed else ("subtitled" if entry_subtitled else "original")

                            entry_format = screen_format
                            if date_entry.get("IsAtmos3D"):
                                entry_format = "ATMOS 3D"
                            elif date_entry.get("IsAtmos2D"):
                                entry_format = "ATMOS"
                            elif date_entry.get("Is3D"):
                                entry_format = "3D"

                            event_id = date_entry.get("EventId", "")
                            all_parsed.append({
                                "movie_title": movie_title,
                                "cinema_name": cinema_name,
                                "city": city,
                                "showtime": showtime,
                                "hall": "",
                                "format": entry_format,
                                "language": language,
                                "booking_url": "",
                                "theater_id": theater_id,
                                "event_id": event_id,
                            })
                        except Exception:
                            continue

            except Exception as e:
                logger.debug(f"[Hot Cinema] API call failed for movie {movie_id} date {date_str}: {e}")
                continue

            await asyncio.sleep(0.3)  # Brief pause between date calls

        logger.info(f"[Hot Cinema] '{movie_title}' (id={movie_id}): {len(all_parsed)} screenings from API over {days} days")
        return all_parsed

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
    # Extract booking URLs from movie page DOM
    # ------------------------------------------------------------------

    async def _extract_booking_urls_from_movie_page(
        self, page: Page, movie_url: str, movie_title: str
    ) -> list[dict]:
        """Navigate to movie page and extract ticket booking URLs.

        The Hot Cinema movie page uses JavaScript click handlers (not <a> hrefs)
        to open ticket pages. We click showtime elements and intercept the
        resulting navigation/popup to discover the correct booking URLs.

        Returns list of dicts with keys: booking_url, time_text, context_text.
        """
        results: list[dict] = []
        try:
            await self._open_url(page, movie_url, wait_for_network=True)
            await asyncio.sleep(2)
            # Scroll to trigger dynamic loading of screening section
            await page.evaluate("window.scrollBy(0, 800)")
            await asyncio.sleep(2)
            await page.evaluate("window.scrollBy(0, 500)")
            await asyncio.sleep(1)

            # Debug: dump page structure for showtime elements
            page_text = await page.inner_text("body")
            # Find time patterns (HH:MM) on the page
            time_matches = re.findall(r'\b\d{2}:\d{2}\b', page_text)
            logger.warning(
                f"[Hot Cinema] '{movie_title}' page times found: {time_matches[:20]}"
            )

            # Strategy 1: Find <a> links to tickets.hotcinema.co.il
            ticket_links = await page.query_selector_all(
                'a[href*="tickets.hotcinema.co.il"]'
            )
            for link in ticket_links:
                try:
                    href = await link.get_attribute("href") or ""
                    if not href or "/site/" not in href:
                        continue
                    link_text = (await link.inner_text()).strip()
                    results.append({
                        "booking_url": href,
                        "time_text": link_text,
                        "context_text": "",
                    })
                except Exception:
                    continue

            # Strategy 2: Check onclick / data attributes
            if not results:
                buttons = await page.query_selector_all(
                    '[onclick*="tickets"], [data-url*="tickets"], '
                    '[data-href*="tickets"], [data-booking-url]'
                )
                for btn in buttons:
                    try:
                        onclick = await btn.get_attribute("onclick") or ""
                        data_url = (
                            await btn.get_attribute("data-url")
                            or await btn.get_attribute("data-href")
                            or await btn.get_attribute("data-booking-url")
                            or ""
                        )
                        raw = data_url or onclick
                        match = re.search(
                            r'https?://tickets\.hotcinema\.co\.il/site/\d+', raw
                        )
                        if match:
                            btn_text = (await btn.inner_text()).strip()
                            results.append({
                                "booking_url": match.group(0),
                                "time_text": btn_text,
                                "context_text": "",
                            })
                    except Exception:
                        continue

            # Strategy 3: Click showtime <a> elements and intercept popup/navigation
            if not results:
                # Scroll to top first, then find showtime anchors
                await page.evaluate("window.scrollTo(0, 0)")
                await asyncio.sleep(1)

                # Count how many showtime <a> tags exist (href with # and HH:MM text)
                num_showtimes = await page.evaluate("""() => {
                    const timePattern = /^\\d{2}:\\d{2}$/;
                    let count = 0;
                    for (const a of document.querySelectorAll('a')) {
                        if (timePattern.test(a.textContent.trim()) && (a.href || '').includes('#'))
                            count++;
                    }
                    return count;
                }""")

                logger.warning(
                    f"[Hot Cinema] '{movie_title}': found {num_showtimes} showtime anchors"
                )

                # Click each showtime using JS scrollIntoView + click
                for i in range(min(num_showtimes, 15)):
                    captured_urls: list[str] = []
                    intercepted_urls: list[str] = []

                    async def on_popup(popup_page):
                        try:
                            await popup_page.wait_for_load_state("domcontentloaded", timeout=10000)
                            final_url = popup_page.url
                        except Exception:
                            final_url = popup_page.url
                        captured_urls.append(final_url)
                        logger.warning(f"[Hot Cinema] Popup: {final_url}")
                        try:
                            await popup_page.close()
                        except Exception:
                            pass

                    async def intercept_tickets(route):
                        intercepted_urls.append(route.request.url)
                        logger.warning(f"[Hot Cinema] Intercepted: {route.request.url}")
                        await route.abort()

                    page.on("popup", on_popup)
                    await page.route("**/tickets.hotcinema.co.il/**", intercept_tickets)

                    # Use JS to find the i-th showtime anchor, scroll to it, and click
                    click_result = await page.evaluate(f"""(index) => {{
                        const timePattern = /^\\d{{2}}:\\d{{2}}$/;
                        const showtimes = [];
                        for (const a of document.querySelectorAll('a')) {{
                            if (timePattern.test(a.textContent.trim()) && (a.href || '').includes('#'))
                                showtimes.push(a);
                        }}
                        if (index >= showtimes.length) return null;
                        const el = showtimes[index];
                        el.scrollIntoView({{ block: 'center' }});
                        const text = el.textContent.trim();
                        el.click();
                        return text;
                    }}""", i)

                    if click_result is None:
                        break

                    time_text = click_result
                    logger.info(f"[Hot Cinema] Clicked showtime #{i}: '{time_text}'")
                    await asyncio.sleep(3)

                    # Check if main page navigated
                    current_url = page.url
                    if "tickets.hotcinema.co.il" in current_url:
                        captured_urls.append(current_url)
                        logger.warning(f"[Hot Cinema] Page navigated to: {current_url}")

                    page.remove_listener("popup", on_popup)
                    await page.unroute("**/tickets.hotcinema.co.il/**")

                    # Collect any discovered booking URLs (use full URL with query params)
                    for url in captured_urls + intercepted_urls:
                        if "tickets.hotcinema.co.il/site/" in url:
                            # Skip static assets (js, css, fonts)
                            if any(ext in url for ext in ['.js', '.css', '.woff', '.ttf', '.png', '.jpg']):
                                continue
                            if not any(r["booking_url"] == url for r in results):
                                results.append({
                                    "booking_url": url,
                                    "time_text": time_text,
                                    "context_text": "",
                                })
                                logger.warning(
                                    f"[Hot Cinema] Discovered: {url} "
                                    f"(time: {time_text})"
                                )

                    # If page navigated away, go back to movie page
                    if "movie/" not in page.url:
                        await self._open_url(page, movie_url, wait_for_network=True)
                        await asyncio.sleep(2)
            # Log results
            if results:
                logger.info(
                    f"[Hot Cinema] '{movie_title}': found {len(results)} booking URLs"
                )
                for r in results[:5]:
                    logger.warning(
                        f"[Hot Cinema] Booking URL: {r['booking_url']} "
                        f"(time: {r['time_text']})"
                    )
            else:
                # Dump all links for debugging
                all_hrefs = await page.evaluate("""() => {
                    return Array.from(document.querySelectorAll('a[href]'))
                        .slice(0, 20)
                        .map(a => ({href: a.href, text: a.textContent.trim().substring(0, 50)}));
                }""")
                logger.warning(
                    f"[Hot Cinema] '{movie_title}': no booking URLs found. "
                    f"All page links: {all_hrefs}"
                )

        except Exception as e:
            logger.warning(
                f"[Hot Cinema] Failed to extract booking URLs from {movie_url}: {e}"
            )

        return results

    # ------------------------------------------------------------------
    # Navigate ticket purchase flow to reach seat map
    # ------------------------------------------------------------------

    async def _navigate_to_seat_map(self, page: Page, booking_url: str,
                                    movie_title: str = "", screening_time: str = "",
                                    branch: str = "") -> tuple[int, int, list]:
        """Navigate from booking URL through ticket selection to seat map.

        Flow: booking_url → ticket page → click + → click המשך → seat map
        URL pattern: tickets.hotcinema.co.il/site/{id}?code={id}-{EventId}&...
        Seat map: tickets.hotcinema.co.il/site/{id}/seats
        """
        try:
            logger.warning(f"[Hot Cinema] Seat map: navigating to {booking_url}")
            await self._open_url(page, booking_url, wait_for_network=True)
            await asyncio.sleep(4)  # SPA needs time to render

            current_url = page.url
            logger.warning(f"[Hot Cinema] Seat map: loaded {current_url}")

            # Save debug screenshots - step 1: booking page
            try:
                await page.screenshot(path=_TICKET_DEBUG_SCREENSHOT)
                await page.screenshot(path=_debug_screenshot_path("step1_booking", movie_title, screening_time, branch=branch))
                logger.info(f"[Hot Cinema] Step 1 screenshot saved (booking page)")
            except Exception:
                pass

            # Detect error page
            if "/error" in current_url:
                try:
                    body_text = (await page.inner_text("body"))[:500]
                    logger.warning(f"[Hot Cinema] Ticket page error: {body_text}")
                except Exception:
                    pass
                return 0, 0, []

            # Check if already on seat map
            if "/seats" in current_url:
                logger.info("[Hot Cinema] Seat map: already on seat page")
                return await self._count_seats_on_page(page, movie_title=movie_title, screening_time=screening_time, branch=branch)

            # Shortcut: try navigating directly to /seats
            # Extract base site URL (e.g., /site/1183)
            site_match = re.search(r'(https?://tickets\.hotcinema\.co\.il/site/\d+)', current_url)
            if site_match:
                seats_url = f"{site_match.group(1)}/seats"
                logger.info(f"[Hot Cinema] Seat map: trying shortcut to {seats_url}")
                await page.goto(seats_url, wait_until="networkidle", timeout=15000)
                await asyncio.sleep(3)

                if "/seats" in page.url:
                    logger.info(f"[Hot Cinema] Seat map: shortcut worked → {page.url}")
                    try:
                        await page.screenshot(path=_TICKET_DEBUG_SCREENSHOT)
                        await page.screenshot(path=_debug_screenshot_path("step4_seat_map_shortcut", movie_title, screening_time, branch=branch))
                    except Exception:
                        pass
                    total, sold, sold_positions = await self._count_seats_on_page(page, movie_title=movie_title, screening_time=screening_time, branch=branch)
                    logger.info(f"[Hot Cinema] Seat map: counted {sold}/{total} seats")
                    return total, sold, sold_positions

                # Shortcut didn't work, go back to ticket page
                logger.info("[Hot Cinema] Seat map: shortcut failed, going through ticket flow")
                await self._open_url(page, booking_url, wait_for_network=True)
                await asyncio.sleep(4)

            # Log page structure for debugging
            page_info = await page.evaluate("""() => {
                const body = document.body;
                if (!body) return {error: 'no body'};

                // Collect all visible buttons/clickable elements
                const clickables = [];
                for (const el of document.querySelectorAll('button, a, [role="button"], input[type="button"], input[type="submit"]')) {
                    const rect = el.getBoundingClientRect();
                    if (rect.width > 0 && rect.height > 0) {
                        clickables.push({
                            tag: el.tagName,
                            text: el.textContent.trim().substring(0, 60),
                            cls: (el.className || '').toString().substring(0, 80),
                            href: (el.href || '').substring(0, 80),
                        });
                    }
                }

                // Check for any elements containing "+"
                const plusEls = [];
                for (const el of document.querySelectorAll('*')) {
                    if (el.children.length === 0 && el.textContent.trim() === '+') {
                        const rect = el.getBoundingClientRect();
                        if (rect.width > 0 && rect.height > 0) {
                            plusEls.push({
                                tag: el.tagName,
                                cls: (el.className || '').toString().substring(0, 80),
                                parent: el.parentElement ? el.parentElement.tagName + '.' + (el.parentElement.className || '').toString().substring(0, 40) : '',
                            });
                        }
                    }
                }

                return {
                    url: window.location.href,
                    title: document.title,
                    clickableCount: clickables.length,
                    clickables: clickables.slice(0, 20),
                    plusElements: plusEls.slice(0, 5),
                    bodyText: body.innerText.substring(0, 500),
                };
            }""")
            logger.warning(f"[Hot Cinema] Ticket page structure: {page_info}")

            # Click the first "+" button to add a regular ticket
            # Try multiple strategies to find the + button
            plus_clicked = await page.evaluate("""() => {
                // Strategy 1: leaf elements with just "+" text
                for (const el of document.querySelectorAll('*')) {
                    if (el.children.length === 0 && el.textContent.trim() === '+') {
                        const rect = el.getBoundingClientRect();
                        if (rect.width > 0 && rect.height > 0 && rect.width < 200) {
                            el.scrollIntoView({block: 'center'});
                            el.click();
                            return 'leaf:' + el.tagName;
                        }
                    }
                }

                // Strategy 2: buttons/clickables with "+" text (including parent)
                for (const el of document.querySelectorAll('button, a, span, div, [role="button"], i, svg')) {
                    const text = el.textContent.trim();
                    if (text === '+' || text === '＋' || text === '+1') {
                        const rect = el.getBoundingClientRect();
                        if (rect.width > 0 && rect.height > 0 && rect.width < 200) {
                            el.scrollIntoView({block: 'center'});
                            el.click();
                            return 'clickable:' + el.tagName;
                        }
                    }
                }

                // Strategy 3: aria-label or title containing "add" or "plus" or "הוסף"
                for (const el of document.querySelectorAll('[aria-label*="add" i], [aria-label*="plus" i], [aria-label*="הוסף"], [title*="הוסף"]')) {
                    const rect = el.getBoundingClientRect();
                    if (rect.width > 0 && rect.height > 0) {
                        el.scrollIntoView({block: 'center'});
                        el.click();
                        return 'aria:' + el.tagName;
                    }
                }

                // Strategy 4: class names containing "plus", "add", "increase", "increment"
                for (const el of document.querySelectorAll('[class*="plus"], [class*="Plus"], [class*="add"], [class*="Add"], [class*="increase"], [class*="increment"], [class*="Increment"]')) {
                    const rect = el.getBoundingClientRect();
                    if (rect.width > 0 && rect.height > 0 && rect.width < 200) {
                        el.scrollIntoView({block: 'center'});
                        el.click();
                        return 'class:' + el.tagName + '.' + el.className;
                    }
                }

                return false;
            }""")

            if plus_clicked:
                logger.info(f"[Hot Cinema] Seat map: clicked + button via {plus_clicked}")
                await asyncio.sleep(2)
                # Step 2 screenshot: after clicking +
                try:
                    await page.screenshot(path=_debug_screenshot_path("step2_plus_click", movie_title, screening_time, branch=branch))
                    logger.info("[Hot Cinema] Step 2 screenshot saved (after + click)")
                except Exception:
                    pass
            else:
                logger.warning("[Hot Cinema] Seat map: could not find + button")

            # Click "המשך" (continue) button
            proceed_clicked = await page.evaluate("""() => {
                // Strategy 1: exact text match
                for (const el of document.querySelectorAll('button, a, [role="button"], input[type="submit"]')) {
                    const text = el.textContent.trim();
                    if (text === 'המשך' || text === 'המשך לבחירת מושבים') {
                        el.scrollIntoView({block: 'center'});
                        el.click();
                        return 'exact:' + el.tagName;
                    }
                }

                // Strategy 2: contains המשך
                for (const el of document.querySelectorAll('button, a, [role="button"]')) {
                    const text = el.textContent.trim();
                    if (text.includes('המשך')) {
                        el.scrollIntoView({block: 'center'});
                        el.click();
                        return 'contains:' + el.tagName;
                    }
                }

                // Strategy 3: any "continue" / "next" / "proceed" button
                for (const el of document.querySelectorAll('button, a, [role="button"]')) {
                    const text = el.textContent.trim().toLowerCase();
                    if (text === 'continue' || text === 'next' || text === 'proceed') {
                        el.scrollIntoView({block: 'center'});
                        el.click();
                        return 'en:' + el.tagName;
                    }
                }

                return false;
            }""")

            if proceed_clicked:
                logger.info(f"[Hot Cinema] Seat map: clicked המשך button via {proceed_clicked}")
                await asyncio.sleep(2)
                try:
                    await page.wait_for_load_state("networkidle", timeout=10000)
                except Exception:
                    pass
                await asyncio.sleep(3)
                # Step 3 screenshot: after clicking המשך
                try:
                    await page.screenshot(path=_debug_screenshot_path("step3_continue", movie_title, screening_time, branch=branch))
                    logger.info("[Hot Cinema] Step 3 screenshot saved (after continue)")
                except Exception:
                    pass
            else:
                logger.warning("[Hot Cinema] Seat map: could not find המשך button")

            logger.info(f"[Hot Cinema] Seat map: now on {page.url}")
            # Step 4 screenshot: seat map page
            try:
                await page.screenshot(path=_TICKET_DEBUG_SCREENSHOT)
                await page.screenshot(path=_debug_screenshot_path("step4_seat_map", movie_title, screening_time, branch=branch))
                logger.info("[Hot Cinema] Step 4 screenshot saved (seat map page)")
            except Exception:
                pass

            total, sold, sold_positions = await self._count_seats_on_page(page, movie_title=movie_title, screening_time=screening_time, branch=branch)
            logger.info(f"[Hot Cinema] Seat map: counted {sold}/{total} seats")
            return total, sold, sold_positions

        except Exception as e:
            logger.warning(f"[Hot Cinema] Seat map navigation failed: {e}")
            return 0, 0

    # ------------------------------------------------------------------
    # Scrape implementations
    # ------------------------------------------------------------------

    async def scrape_movies(self, on_progress=None) -> list[ScrapedMovie]:
        """Weekly: scrape all movies from all branches + homepage."""
        all_movies: dict[str, ScrapedMovie] = {}
        pw, browser, context = await self._launch_browser()
        try:
            page = await context.new_page()

            # Collect movies from theater pages
            branch_items = list(HOT_CINEMA_BRANCHES.items())
            for idx, (branch_id, branch_info) in enumerate(branch_items):
                if on_progress:
                    on_progress("סורק סניפים", idx + 1, len(branch_items), branch_info["name"])
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

    # Testing limit — set to None for full scrape
    _TEST_MOVIE_LIMIT = None

    async def scrape_screenings(self, on_progress=None) -> list[ScrapedScreening]:
        """Daily: fetch screenings via API (seat counts deferred to ticket updates)."""
        all_screenings: list[ScrapedScreening] = []
        pw, browser, context = await self._launch_browser()
        try:
            page = await context.new_page()

            # Step 1: Collect unique movie URLs from all branches
            movie_urls: dict[str, str] = {}  # title -> URL
            branch_items = list(HOT_CINEMA_BRANCHES.items())
            for idx, (branch_id, branch_info) in enumerate(branch_items):
                if on_progress:
                    on_progress("סורק סניפים", idx + 1, len(branch_items), branch_info["name"])
                movies = await self._scrape_theater_page(page, branch_id, branch_info)
                for m in movies:
                    if m.detail_url and m.title not in movie_urls:
                        movie_urls[m.title] = m.detail_url

            logger.info(f"[Hot Cinema] Found {len(movie_urls)} unique movie URLs")

            # Step 2: Extract movie IDs and apply test limit
            movie_list: list[tuple[str, str, str]] = []  # (title, url, movie_id)
            for title, url in movie_urls.items():
                mid = _extract_movie_id(url)
                if mid:
                    movie_list.append((title, url, mid))

            if self._TEST_MOVIE_LIMIT:
                movie_list = movie_list[:self._TEST_MOVIE_LIMIT]
                logger.info(f"[Hot Cinema] Testing with {len(movie_list)} movies")

            # Step 3: Fetch screenings via direct API calls (7 days)
            all_infos: list[dict] = []
            for idx, (title, url, mid) in enumerate(movie_list):
                if on_progress:
                    on_progress("סורק הקרנות", idx + 1, len(movie_list), title)
                infos = await self._fetch_screenings_api(page, mid, title, days=7)
                all_infos.extend(infos)

            logger.info(f"[Hot Cinema] Total screenings from API: {len(all_infos)}")

            # Step 4: Build screenings (seat counts deferred to scrape_ticket_updates)
            for info in all_infos:
                if info["showtime"] < datetime.now():
                    continue

                screening = ScrapedScreening(
                    movie_title=info["movie_title"],
                    cinema_name=info["cinema_name"],
                    city=info["city"],
                    showtime=info["showtime"],
                    hall=info["hall"],
                    format=info["format"],
                    language=info.get("language", "subtitled"),
                    ticket_price=39.0,
                    total_seats=0,
                    tickets_sold=0,
                )
                screening.revenue = 0
                all_screenings.append(screening)
        finally:
            await browser.close()
            await pw.stop()

        logger.info(f"[Hot Cinema] Daily scrape: {len(all_screenings)} screenings from {len(movie_list)} movies")
        return all_screenings

    async def scrape_ticket_updates(self, on_progress=None, on_screening_update=None) -> list[ScrapedScreening]:
        """Every 5 hours: fetch screenings via API, navigate seat maps for occupancy."""
        all_screenings: list[ScrapedScreening] = []
        pw, browser, context = await self._launch_browser()
        try:
            page = await context.new_page()

            # Collect movie URLs from all branches
            movie_urls: dict[str, str] = {}
            branch_items = list(HOT_CINEMA_BRANCHES.items())
            for idx, (branch_id, branch_info) in enumerate(branch_items):
                if on_progress:
                    on_progress("סורק סניפים", idx + 1, len(branch_items), branch_info["name"])
                movies = await self._scrape_theater_page(page, branch_id, branch_info)
                for m in movies:
                    if m.detail_url and m.title not in movie_urls:
                        movie_urls[m.title] = m.detail_url

            # Extract movie IDs
            movie_list: list[tuple[str, str, str]] = []
            for title, url in movie_urls.items():
                mid = _extract_movie_id(url)
                if mid:
                    movie_list.append((title, url, mid))

            logger.info(f"[Hot Cinema] Ticket update: checking {len(movie_list)} movies")

            if self._TEST_MOVIE_LIMIT:
                movie_list = movie_list[:self._TEST_MOVIE_LIMIT]
                logger.info(f"[Hot Cinema] Ticket update: limited to {len(movie_list)} movies")

            # TheaterID → siteId mapping (discovered from booking URLs)
            theater_to_site: dict[str, str] = {}

            screening_counter = 0
            for title, url, mid in movie_list:
                screening_infos = await self._fetch_screenings_api(page, mid, title, days=7)
                future_infos = [i for i in screening_infos if i["showtime"] >= datetime.now()]

                if not future_infos:
                    continue

                # Discover booking URLs from movie page to learn TheaterID→siteId mapping
                # Only needed if we have theaters we haven't mapped yet
                unmapped_theaters = {
                    str(i.get("theater_id", ""))
                    for i in future_infos
                    if str(i.get("theater_id", "")) and str(i.get("theater_id", "")) not in theater_to_site
                }

                if unmapped_theaters:
                    booking_links = await self._extract_booking_urls_from_movie_page(
                        page, url, title
                    )

                    # Extract siteId from discovered booking URLs and correlate with screenings
                    for bl in booking_links:
                        burl = bl["booking_url"]
                        # URL pattern: /site/{siteId}?code={siteId}-{eventId}&...
                        site_match = re.search(r'/site/(\d+)', burl)
                        code_match = re.search(r'code=(\d+)-(\d+)', burl)
                        if not site_match:
                            continue
                        site_id = site_match.group(1)
                        event_id_from_url = code_match.group(2) if code_match else ""

                        # Match this event_id to a screening to find the TheaterID
                        if event_id_from_url:
                            for info in future_infos:
                                if str(info.get("event_id", "")) == event_id_from_url:
                                    tid = str(info.get("theater_id", ""))
                                    if tid and tid not in theater_to_site:
                                        theater_to_site[tid] = site_id
                                        logger.info(
                                            f"[Hot Cinema] Mapped TheaterID={tid} → siteId={site_id}"
                                        )
                                    break

                    logger.info(
                        f"[Hot Cinema] TheaterID→siteId mapping so far: {theater_to_site}"
                    )

                # Now construct booking URLs for ALL screenings using the mapping
                for idx, info in enumerate(future_infos):
                    screening_counter += 1
                    if on_progress:
                        on_progress("סורק כיסאות", screening_counter, 0, info["movie_title"])

                    total_seats = 0
                    tickets_sold = 0
                    seat_positions = []

                    # Construct booking URL from TheaterID→siteId mapping + EventId
                    tid = str(info.get("theater_id", ""))
                    eid = str(info.get("event_id", ""))
                    site_id = theater_to_site.get(tid, "")

                    booking_url = ""
                    if site_id and eid:
                        booking_url = (
                            f"https://tickets.hotcinema.co.il/site/{site_id}"
                            f"?code={site_id}-{eid}"
                            f"&saleChannelCode=WEB&languageid=he_IL"
                        )

                    if booking_url:
                        try:
                            total, sold, positions = await self._navigate_to_seat_map(
                                page, booking_url,
                                movie_title=info["movie_title"],
                                screening_time=info["showtime"].strftime("%H%M"),
                                branch=info["cinema_name"],
                            )
                            if total > 0:
                                total_seats = total
                                tickets_sold = sold
                                seat_positions = positions
                                logger.info(
                                    f"  [{info['cinema_name']}] {info['movie_title']} "
                                    f"{info['showtime'].strftime('%d/%m %H:%M')}: "
                                    f"{tickets_sold}/{total_seats} seats"
                                )
                        except Exception as e:
                            logger.debug(f"[Hot Cinema] Seat map failed: {e}")
                    else:
                        logger.debug(
                            f"[Hot Cinema] No booking URL for TheaterID={tid} EventId={eid} "
                            f"(siteId mapping: {'found' if site_id else 'missing'})"
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
                        sold_positions=seat_positions,
                    )
                    screening.revenue = screening.tickets_sold * screening.ticket_price
                    all_screenings.append(screening)

                    # Save to DB immediately so dashboard updates in real-time
                    if on_screening_update:
                        try:
                            on_screening_update(screening)
                        except Exception as e:
                            logger.debug(f"[Hot Cinema] Screening save callback failed: {e}")
        finally:
            await browser.close()
            await pw.stop()

        logger.info(f"[Hot Cinema] Ticket update: {len(all_screenings)} screenings counted")
        return all_screenings
