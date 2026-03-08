"""
Seed the database with demo data for development and demonstration.
This provides realistic Israeli cinema data so the dashboard works immediately.
"""
import random
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from database import engine, SessionLocal, Base
from models.models import CinemaChain, Cinema, Movie, Screening

CHAINS = [
    {"name": "Cinema City", "name_he": "סינמה סיטי / יס פלאנט", "website": "https://www.cinema-city.co.il"},
    {"name": "Hot Cinema", "name_he": "הוט סינמה", "website": "https://hotcinema.co.il"},
    {"name": "Lev Cinema", "name_he": "לב קולנוע", "website": "https://www.lfrj.co.il"},
    {"name": "Globus Max", "name_he": "גלובוס מקס", "website": "https://www.globusmax.co.il"},
]

CINEMAS = [
    # Cinema City / Yes Planet
    {"chain": "Cinema City", "name": "Yes Planet Rishon LeZion", "name_he": "יס פלאנט ראשון לציון", "city": "Rishon LeZion", "city_he": "ראשון לציון", "halls": 18},
    {"chain": "Cinema City", "name": "Yes Planet Haifa", "name_he": "יס פלאנט חיפה", "city": "Haifa", "city_he": "חיפה", "halls": 14},
    {"chain": "Cinema City", "name": "Yes Planet Jerusalem", "name_he": "יס פלאנט ירושלים", "city": "Jerusalem", "city_he": "ירושלים", "halls": 16},
    {"chain": "Cinema City", "name": "Yes Planet Beer Sheva", "name_he": "יס פלאנט באר שבע", "city": "Beer Sheva", "city_he": "באר שבע", "halls": 12},
    {"chain": "Cinema City", "name": "Cinema City Glilot", "name_he": "סינמה סיטי גלילות", "city": "Ramat HaSharon", "city_he": "רמת השרון", "halls": 20},
    {"chain": "Cinema City", "name": "Cinema City Netanya", "name_he": "סינמה סיטי נתניה", "city": "Netanya", "city_he": "נתניה", "halls": 10},
    {"chain": "Cinema City", "name": "Cinema City Ashdod", "name_he": "סינמה סיטי אשדוד", "city": "Ashdod", "city_he": "אשדוד", "halls": 10},
    # Hot Cinema
    {"chain": "Hot Cinema", "name": "Hot Cinema Kfar Saba", "name_he": "הוט סינמה כפר סבא", "city": "Kfar Saba", "city_he": "כפר סבא", "halls": 8},
    {"chain": "Hot Cinema", "name": "Hot Cinema Kiryat Ono", "name_he": "הוט סינמה קריית אונו", "city": "Kiryat Ono", "city_he": "קריית אונו", "halls": 7},
    {"chain": "Hot Cinema", "name": "Hot Cinema Haifa", "name_he": "הוט סינמה חיפה", "city": "Haifa", "city_he": "חיפה", "halls": 9},
    {"chain": "Hot Cinema", "name": "Hot Cinema Herzliya", "name_he": "הוט סינמה הרצליה", "city": "Herzliya", "city_he": "הרצליה", "halls": 8},
    {"chain": "Hot Cinema", "name": "Hot Cinema Petah Tikva", "name_he": "הוט סינמה פתח תקווה", "city": "Petah Tikva", "city_he": "פתח תקווה", "halls": 10},
    {"chain": "Hot Cinema", "name": "Hot Cinema Eilat", "name_he": "הוט סינמה אילת", "city": "Eilat", "city_he": "אילת", "halls": 6},
    # Lev Cinema
    {"chain": "Lev Cinema", "name": "Lev Smadar", "name_he": "לב סמדר", "city": "Jerusalem", "city_he": "ירושלים", "halls": 2},
    {"chain": "Lev Cinema", "name": "Lev Dizengoff", "name_he": "לב דיזנגוף", "city": "Tel Aviv", "city_he": "תל אביב", "halls": 3},
    # Globus Max
    {"chain": "Globus Max", "name": "Globus Max Rishon LeZion", "name_he": "גלובוס מקס ראשון לציון", "city": "Rishon LeZion", "city_he": "ראשון לציון", "halls": 8},
    {"chain": "Globus Max", "name": "Globus Max Holon", "name_he": "גלובוס מקס חולון", "city": "Holon", "city_he": "חולון", "halls": 6},
]

MOVIES = [
    {"title": "Captain America: Brave New World", "title_he": "קפטן אמריקה: עולם חדש ואמיץ", "genre": "Action", "duration": 148, "release_date": "2025-02-14", "rating": "PG-13", "director": "Julius Onah"},
    {"title": "Mickey 17", "title_he": "מיקי 17", "genre": "Sci-Fi", "duration": 137, "release_date": "2025-03-07", "rating": "R", "director": "Bong Joon-ho"},
    {"title": "Snow White", "title_he": "שלגייה", "genre": "Fantasy", "duration": 115, "release_date": "2025-03-21", "rating": "PG", "director": "Marc Webb"},
    {"title": "Thunderbolts*", "title_he": "ת'אנדרבולטס*", "genre": "Action", "duration": 127, "release_date": "2025-05-02", "rating": "PG-13", "director": "Jake Schreier"},
    {"title": "Lilo & Stitch", "title_he": "לילו וסטיץ'", "genre": "Family", "duration": 108, "release_date": "2025-05-23", "rating": "PG", "director": "Dean Fleischer Camp"},
    {"title": "Mission: Impossible 8", "title_he": "משימה בלתי אפשרית 8", "genre": "Action", "duration": 165, "release_date": "2025-05-23", "rating": "PG-13", "director": "Christopher McQuarrie"},
    {"title": "Jurassic World Rebirth", "title_he": "עולם היורה: לידה מחדש", "genre": "Action", "duration": 142, "release_date": "2025-07-02", "rating": "PG-13", "director": "Gareth Edwards"},
    {"title": "The Amateur", "title_he": "החובבן", "genre": "Thriller", "duration": 120, "release_date": "2025-04-11", "rating": "R", "director": "James Hawes"},
    {"title": "A Minecraft Movie", "title_he": "סרט מיינקראפט", "genre": "Adventure", "duration": 110, "release_date": "2025-04-04", "rating": "PG", "director": "Jared Hess"},
    {"title": "Sinners", "title_he": "חוטאים", "genre": "Horror", "duration": 135, "release_date": "2025-04-18", "rating": "R", "director": "Ryan Coogler"},
    {"title": "Ballerina", "title_he": "בלרינה", "genre": "Action", "duration": 118, "release_date": "2025-06-06", "rating": "R", "director": "Len Wiseman"},
    {"title": "How to Train Your Dragon", "title_he": "איך לאלף דרקון", "genre": "Fantasy", "duration": 125, "release_date": "2025-06-13", "rating": "PG", "director": "Dean DeBlois"},
    {"title": "Elio", "title_he": "אליו", "genre": "Animation", "duration": 100, "release_date": "2025-06-20", "rating": "PG", "director": "Adrian Molina"},
    {"title": "The Bride", "title_he": "הכלה", "genre": "Horror", "duration": 108, "release_date": "2025-09-26", "rating": "PG-13", "director": "Maggie Gyllenhaal"},
    {"title": "Wicked Part Two", "title_he": "ויקד חלק 2", "genre": "Musical", "duration": 160, "release_date": "2025-11-21", "rating": "PG", "director": "Jon M. Chu"},
]

FORMATS = ["2D", "3D", "IMAX", "4DX", "ScreenX"]
SHOWTIMES = ["10:00", "12:30", "14:00", "16:30", "19:00", "21:30", "23:45"]


def seed_database():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()

    # Skip if data already exists
    if db.query(Movie).count() > 0:
        print("Database already seeded, skipping.")
        db.close()
        return

    try:
        # Create chains
        chain_map = {}
        for c in CHAINS:
            chain = CinemaChain(**c)
            db.add(chain)
            db.flush()
            chain_map[c["name"]] = chain.id

        # Create cinemas
        cinema_objs = []
        for cin in CINEMAS:
            cinema = Cinema(
                chain_id=chain_map[cin["chain"]],
                name=cin["name"],
                name_he=cin["name_he"],
                city=cin["city"],
                city_he=cin["city_he"],
                halls_count=cin["halls"],
            )
            db.add(cinema)
            db.flush()
            cinema_objs.append(cinema)

        # Create movies
        movie_objs = []
        for m in MOVIES:
            movie = Movie(
                title=m["title"],
                title_he=m["title_he"],
                genre=m["genre"],
                duration_minutes=m["duration"],
                release_date=m["release_date"],
                rating=m["rating"],
                director=m["director"],
            )
            db.add(movie)
            db.flush()
            movie_objs.append(movie)

        # Generate screenings for the past 14 days
        random.seed(42)
        now = datetime.now()

        for day_offset in range(-14, 1):
            date = now + timedelta(days=day_offset)

            for cinema in cinema_objs:
                # Each cinema shows a subset of movies
                num_movies = random.randint(4, min(8, len(movie_objs)))
                shown_movies = random.sample(movie_objs, num_movies)

                for movie in shown_movies:
                    # 1-3 screenings per movie per cinema per day
                    num_screenings = random.randint(1, 3)
                    chosen_times = random.sample(SHOWTIMES, min(num_screenings, len(SHOWTIMES)))

                    for time_str in chosen_times:
                        hour, minute = map(int, time_str.split(":"))
                        showtime = date.replace(hour=hour, minute=minute, second=0, microsecond=0)

                        fmt = random.choices(
                            FORMATS, weights=[50, 15, 15, 10, 10], k=1
                        )[0]

                        total_seats = random.choice([120, 150, 180, 200, 250, 300, 350])
                        if fmt == "IMAX":
                            total_seats = random.choice([300, 350, 400])
                        elif fmt == "4DX":
                            total_seats = random.choice([80, 100, 120])

                        # Simulate occupancy based on various factors
                        base_occupancy = random.uniform(0.2, 0.85)

                        # Weekend boost
                        if date.weekday() in (4, 5):  # Friday, Saturday
                            base_occupancy = min(1.0, base_occupancy * 1.3)

                        # Evening boost
                        if hour >= 19:
                            base_occupancy = min(1.0, base_occupancy * 1.15)

                        # New release boost (first week)
                        if movie.release_date:
                            try:
                                rel = datetime.strptime(movie.release_date, "%Y-%m-%d")
                                days_since = (date - rel).days
                                if 0 <= days_since <= 7:
                                    base_occupancy = min(1.0, base_occupancy * 1.4)
                            except ValueError:
                                pass

                        tickets_sold = int(total_seats * base_occupancy)
                        ticket_price = random.choice([35.0, 39.0, 42.0, 45.0, 49.0, 55.0])
                        if fmt == "IMAX":
                            ticket_price += 15
                        elif fmt == "3D":
                            ticket_price += 8
                        elif fmt == "4DX":
                            ticket_price += 20

                        screening = Screening(
                            movie_id=movie.id,
                            cinema_id=cinema.id,
                            showtime=showtime,
                            hall=f"Hall {random.randint(1, cinema.halls_count or 8)}",
                            format=fmt,
                            language=random.choice(["subtitled", "dubbed", "original"]),
                            ticket_price=ticket_price,
                            tickets_sold=tickets_sold,
                            total_seats=total_seats,
                            revenue=tickets_sold * ticket_price,
                        )
                        db.add(screening)

        db.commit()
        print(f"Seeded: {len(CHAINS)} chains, {len(cinema_objs)} cinemas, "
              f"{len(movie_objs)} movies, ~{15 * len(cinema_objs) * 6 * 2} screenings")

    except Exception as e:
        db.rollback()
        print(f"Seed error: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    seed_database()
