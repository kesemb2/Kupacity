"""
Scraper for Hot Cinema chain using SeleniumBase in UC (Undetected
Chromedriver) mode.

UC mode patches the chromedriver binary at runtime so that Cloudflare,
DataDome, and similar bot-detection services cannot fingerprint it as
automated.  This replaces the previous Playwright + stealth approach
which was still getting 403'd.

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
import time
from datetime import datetime, timedelta

from seleniumbase import SB
from selenium.webdriver.common.by import By

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
    # SB context builder
    # ------------------------------------------------------------------

    @staticmethod
    def _sb_kwargs() -> dict:
        """Build keyword arguments for ``SB(...)``."""
        kwargs: dict = {
            "uc": True,
            "headless": True,
            "locale_code": "en-US",
        }

        proxy = os.environ.get("SCRAPER_PROXY_SERVER")
        if proxy:
            # SB accepts "host:port" or "user:pass@host:port"
            # Strip the scheme if present (SB adds its own).
            cleaned = re.sub(r"^https?://", "", proxy)
            kwargs["proxy"] = cleaned
            logger.info(f"[Hot Cinema] Using proxy: {proxy}")

        return kwargs

    # ------------------------------------------------------------------
    # Anti-detection helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _human_delay(lo: float = 2.0, hi: float = 7.0):
        """Sleep for a random duration to mimic human pacing."""
        time.sleep(random.uniform(lo, hi))

    @staticmethod
    def _simulate_human(sb):
        """Scroll a bit and move mouse to look human."""
        try:
            scroll_y = random.randint(150, 500)
            sb.execute_script(f"window.scrollBy(0, {scroll_y});")
            time.sleep(random.uniform(0.3, 0.8))
            sb.execute_script(f"window.scrollBy(0, -{random.randint(50, scroll_y)});")
            time.sleep(random.uniform(0.2, 0.5))
        except Exception:
            pass

    def _open_url(self, sb, url: str, *, take_debug_screenshot: bool = False):
        """Navigate with UC reconnect, random delay, and human jitter.

        ``uc_open_with_reconnect`` lets SeleniumBase handle Cloudflare
        challenge pages automatically (it waits, clicks the checkbox if
        needed, and reconnects the driver).
        """
        self._human_delay()
        try:
            sb.uc_open_with_reconnect(url, reconnect_time=6)
        except Exception:
            # Fallback for sites that don't trigger a challenge
            sb.open(url)

        if take_debug_screenshot:
            try:
                sb.save_screenshot(_DEBUG_SCREENSHOT)
                logger.info(f"[Hot Cinema] Debug screenshot saved → {_DEBUG_SCREENSHOT}")
            except Exception as e:
                logger.debug(f"Screenshot failed: {e}")

        self._simulate_human(sb)

    # ------------------------------------------------------------------
    # Selenium-based element helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _find_elements(sb, css: str) -> list:
        """Wrapper around find_elements that never raises."""
        try:
            return sb.find_elements(css)
        except Exception:
            return []

    @staticmethod
    def _el_text(el) -> str:
        try:
            return (el.text or "").strip()
        except Exception:
            return ""

    @staticmethod
    def _el_attr(el, attr: str) -> str:
        try:
            return (el.get_attribute(attr) or "").strip()
        except Exception:
            return ""

    # ------------------------------------------------------------------
    # Seat counting
    # ------------------------------------------------------------------

    def _count_seats_on_page(self, sb) -> tuple[int, int]:
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
                seats = self._find_elements(sb, selector)
                if len(seats) < 5:
                    continue

                total = len(seats)
                for seat in seats:
                    classes = self._el_attr(seat, "class").lower()
                    data_status = self._el_attr(seat, "data-status").lower()
                    aria_disabled = self._el_attr(seat, "aria-disabled").lower()
                    style = self._el_attr(seat, "style").lower()

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

                    fill = self._el_attr(seat, "fill").lower()
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
                body_text = sb.get_text("body")
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

    def _get_seat_count_for_screening(self, sb, screening_url: str) -> tuple[int, int]:
        try:
            self._open_url(sb, screening_url)
            time.sleep(3)
            return self._count_seats_on_page(sb)
        except Exception as e:
            logger.warning(f"Seat count failed for {screening_url}: {e}")
            return 0, 0

    # ------------------------------------------------------------------
    # Theater page scraping
    # ------------------------------------------------------------------

    def _scrape_theater_page(self, sb, branch_id: str,
                              branch_info: dict) -> tuple[list[ScrapedMovie], list[dict]]:
        movies: list[ScrapedMovie] = []
        screening_infos: list[dict] = []

        url = f"{BASE_URL}/theater/{branch_id}/{branch_info['slug']}"
        try:
            self._open_url(sb, url, take_debug_screenshot=(branch_id == "1"))

            movie_elements = self._find_elements(
                sb,
                '[class*="movie"], [class*="Movie"], [class*="film"], [class*="Film"], '
                '[data-movie], article, .card, [class*="item"]',
            )

            for elem in movie_elements:
                try:
                    title_els = elem.find_elements(
                        By.CSS_SELECTOR,
                        'h2, h3, h4, [class*="title"], [class*="name"], [class*="Title"], [class*="Name"]',
                    )
                    if not title_els:
                        continue
                    title = self._el_text(title_els[0])
                    if not title or len(title) < 2:
                        continue

                    poster_url = ""
                    imgs = elem.find_elements(By.CSS_SELECTOR, "img")
                    if imgs:
                        poster_url = self._el_attr(imgs[0], "src")
                        if poster_url and not poster_url.startswith("http"):
                            poster_url = f"{BASE_URL}{poster_url}"

                    movies.append(ScrapedMovie(title=title, title_he=title, poster_url=poster_url))

                    showtime_els = elem.find_elements(
                        By.CSS_SELECTOR,
                        'a[href*="tickets"], a[href*="booking"], a[href*="order"], '
                        'a[href*="seats"], a[href*="site"], '
                        '[class*="showtime"], [class*="time"], [class*="screening"], '
                        '[class*="Showtime"], [class*="Time"], [class*="Screening"], '
                        'button[class*="time"], a[class*="time"]',
                    )

                    for st_el in showtime_els:
                        try:
                            time_text = self._el_text(st_el)
                            time_match = re.search(r"(\d{1,2}):(\d{2})", time_text)
                            if not time_match:
                                continue

                            hour, minute = int(time_match.group(1)), int(time_match.group(2))
                            showtime = datetime.now().replace(
                                hour=hour, minute=minute, second=0, microsecond=0,
                            )
                            if showtime < datetime.now():
                                showtime += timedelta(days=1)

                            format_text = time_text.upper()
                            parent_text = ""
                            try:
                                parent = sb.execute_script(
                                    "return arguments[0].closest("
                                    "'[class*=format],[class*=Format],[class*=type],[class*=Type]'"
                                    ")",
                                    st_el,
                                )
                                if parent:
                                    parent_text = (parent.text or "").upper()
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

                            hall = self._el_attr(st_el, "data-hall")

                            booking_url = ""
                            href = self._el_attr(st_el, "href")
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
                                    self._el_attr(st_el, "data-url")
                                    or self._el_attr(st_el, "data-href")
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

    def _scrape_movie_detail(self, sb, movie_path: str) -> ScrapedMovie | None:
        try:
            url = f"{BASE_URL}{movie_path}" if movie_path.startswith("/") else movie_path
            self._open_url(sb, url)

            title = ""
            title_els = self._find_elements(sb, 'h1, [class*="movieTitle"], [class*="MovieTitle"]')
            if title_els:
                title = self._el_text(title_els[0])

            genre = ""
            genre_els = self._find_elements(sb, '[class*="genre"], [class*="Genre"]')
            if genre_els:
                genre = self._el_text(genre_els[0])

            duration = 0
            dur_els = self._find_elements(sb, '[class*="duration"], [class*="Duration"], [class*="length"]')
            if dur_els:
                dur_match = re.search(r"(\d+)", self._el_text(dur_els[0]))
                if dur_match:
                    duration = int(dur_match.group(1))

            director = ""
            dir_els = self._find_elements(sb, '[class*="director"], [class*="Director"]')
            if dir_els:
                director = self._el_text(dir_els[0])

            poster_url = ""
            poster_els = self._find_elements(
                sb, '[class*="poster"] img, [class*="Poster"] img, .movie-image img',
            )
            if poster_els:
                poster_url = self._el_attr(poster_els[0], "src")
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
    # Sync implementations (run inside SB context)
    # ------------------------------------------------------------------

    def _sync_scrape_movies(self) -> list[ScrapedMovie]:
        all_movies: dict[str, ScrapedMovie] = {}
        with SB(**self._sb_kwargs()) as sb:
            for branch_id, branch_info in HOT_CINEMA_BRANCHES.items():
                movies, _ = self._scrape_theater_page(sb, branch_id, branch_info)
                for m in movies:
                    if m.title and m.title not in all_movies:
                        all_movies[m.title] = m

            # Movie detail pages from homepage
            try:
                self._open_url(sb, BASE_URL)
                links = self._find_elements(sb, 'a[href*="/movie/"]')
                movie_paths: set[str] = set()
                for link in links:
                    href = self._el_attr(link, "href")
                    if href and "/movie/" in href:
                        movie_paths.add(href)
                for path in list(movie_paths)[:30]:
                    movie = self._scrape_movie_detail(sb, path)
                    if movie and movie.title and movie.title not in all_movies:
                        all_movies[movie.title] = movie
            except Exception as e:
                logger.warning(f"Hot Cinema homepage scrape failed: {e}")

        result = list(all_movies.values())
        if not result:
            logger.warning("[Hot Cinema] No movies found - site may be unreachable or structure changed")
        logger.info(f"[Hot Cinema] Scraped {len(result)} unique movies")
        return result

    def _sync_scrape_screenings(self) -> list[ScrapedScreening]:
        all_screenings: list[ScrapedScreening] = []
        with SB(**self._sb_kwargs()) as sb:
            for branch_id, branch_info in HOT_CINEMA_BRANCHES.items():
                _, screening_infos = self._scrape_theater_page(sb, branch_id, branch_info)

                # Try clicking date buttons for next 7 days
                try:
                    date_buttons = self._find_elements(
                        sb,
                        '[class*="date"], [class*="Date"], [class*="day"], [class*="Day"], '
                        '[data-date], button[class*="calendar"]',
                    )
                    for btn in date_buttons[1:7]:
                        try:
                            btn.click()
                            self._human_delay(1.5, 4.0)
                            _, day_infos = self._scrape_theater_page(sb, branch_id, branch_info)
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

        logger.info(f"[Hot Cinema] Daily scrape: {len(all_screenings)} screenings")
        return all_screenings

    def _sync_scrape_ticket_updates(self) -> list[ScrapedScreening]:
        all_screenings: list[ScrapedScreening] = []
        with SB(**self._sb_kwargs()) as sb:
            for branch_id, branch_info in HOT_CINEMA_BRANCHES.items():
                _, screening_infos = self._scrape_theater_page(sb, branch_id, branch_info)

                for info in screening_infos:
                    if info["showtime"] < datetime.now():
                        continue

                    total_seats = 200
                    tickets_sold = 0

                    booking_url = info.get("booking_url", "")
                    if booking_url:
                        total, sold = self._get_seat_count_for_screening(sb, booking_url)
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
                            self._open_url(
                                sb,
                                f"{BASE_URL}/theater/{branch_id}/{branch_info['slug']}",
                            )
                            time_str = info["showtime"].strftime("%H:%M")
                            time_buttons = self._find_elements(
                                sb, f'[class*="time"]',
                            )
                            for btn in time_buttons:
                                if time_str not in self._el_text(btn):
                                    continue
                                try:
                                    btn.click()
                                    time.sleep(3)
                                    current_url = sb.get_current_url()
                                    if any(kw in current_url for kw in ("seats", "ticket", "booking")):
                                        time.sleep(3)
                                        total, sold = self._count_seats_on_page(sb)
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

        logger.info(f"[Hot Cinema] Ticket update: {len(all_screenings)} screenings counted")
        return all_screenings

    # ------------------------------------------------------------------
    # Public async interface (called by the scheduler / manager)
    # ------------------------------------------------------------------

    async def scrape_movies(self) -> list[ScrapedMovie]:
        """Weekly: scrape all movies (runs sync SB in a thread)."""
        return await asyncio.to_thread(self._sync_scrape_movies)

    async def scrape_screenings(self) -> list[ScrapedScreening]:
        """Daily: scrape screenings (runs sync SB in a thread)."""
        return await asyncio.to_thread(self._sync_scrape_screenings)

    async def scrape_ticket_updates(self) -> list[ScrapedScreening]:
        """Every 5 hours: count seats sold (runs sync SB in a thread)."""
        return await asyncio.to_thread(self._sync_scrape_ticket_updates)
