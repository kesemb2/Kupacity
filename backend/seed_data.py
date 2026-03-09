"""
Seed the database with Hot Cinema chain and its real branches.
Movies and screenings come from the Playwright scraper - not from seed data.
"""
from database import engine, SessionLocal, Base
from models.models import CinemaChain, Cinema

# Real Hot Cinema branches (matching scrapers/hot_cinema.py HOT_CINEMA_BRANCHES)
HOT_CINEMA_CHAIN = {
    "name": "Hot Cinema",
    "name_he": "הוט סינמה",
    "website": "https://hotcinema.co.il",
}

HOT_CINEMA_BRANCHES = [
    {"name": "Hot Cinema Modi'in", "name_he": "הוט סינמה מודיעין", "city": "Modi'in", "city_he": "מודיעין", "halls": 8},
    {"name": "Hot Cinema Kfar Saba", "name_he": "הוט סינמה כפר סבא", "city": "Kfar Saba", "city_he": "כפר סבא", "halls": 8},
    {"name": "Hot Cinema Petah Tikva", "name_he": "הוט סינמה פתח תקווה", "city": "Petah Tikva", "city_he": "פתח תקווה", "halls": 10},
    {"name": "Hot Cinema Rehovot", "name_he": "הוט סינמה רחובות", "city": "Rehovot", "city_he": "רחובות", "halls": 7},
    {"name": "Hot Cinema Haifa", "name_he": "הוט סינמה חיפה", "city": "Haifa", "city_he": "חיפה", "halls": 9},
    {"name": "Hot Cinema Kiryon", "name_he": "הוט סינמה קריון", "city": "Kiryon", "city_he": "קריון", "halls": 7},
    {"name": "Hot Cinema Karmiel", "name_he": "הוט סינמה כרמיאל", "city": "Karmiel", "city_he": "כרמיאל", "halls": 6},
    {"name": "Hot Cinema Nahariya", "name_he": "הוט סינמה נהריה", "city": "Nahariya", "city_he": "נהריה", "halls": 5},
    {"name": "Hot Cinema Ashkelon", "name_he": "הוט סינמה אשקלון", "city": "Ashkelon", "city_he": "אשקלון", "halls": 7},
    {"name": "Hot Cinema Ashdod", "name_he": "הוט סינמה אשדוד", "city": "Ashdod", "city_he": "אשדוד", "halls": 8},
]


def seed_database():
    """Create Hot Cinema chain and its branches. Movies come from the scraper."""
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()

    # Skip if chain already exists
    if db.query(CinemaChain).filter_by(name="Hot Cinema").first():
        print("Hot Cinema chain already exists, skipping seed.")
        db.close()
        return

    try:
        chain = CinemaChain(**HOT_CINEMA_CHAIN)
        db.add(chain)
        db.flush()

        for branch in HOT_CINEMA_BRANCHES:
            cinema = Cinema(
                chain_id=chain.id,
                name=branch["name"],
                name_he=branch["name_he"],
                city=branch["city"],
                city_he=branch["city_he"],
                halls_count=branch["halls"],
            )
            db.add(cinema)

        db.commit()
        print(f"Seeded: Hot Cinema chain with {len(HOT_CINEMA_BRANCHES)} branches")

    except Exception as e:
        db.rollback()
        print(f"Seed error: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    seed_database()
