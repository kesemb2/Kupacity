"""
Seed the database with cinema chains and their real branches.
Movies and screenings come from the Playwright scrapers - not from seed data.
"""
from database import engine, SessionLocal, Base
from models.models import CinemaChain, Cinema

# ── Hot Cinema ───────────────────────────────────────────────────────────────

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

# ── Movieland ────────────────────────────────────────────────────────────────

MOVIELAND_CHAIN = {
    "name": "Movieland",
    "name_he": "מובילנד",
    "website": "https://movieland.co.il",
}

MOVIELAND_BRANCHES = [
    {"name": "Movieland Tel Aviv", "name_he": "מובילנד תל אביב", "city": "Tel Aviv", "city_he": "תל אביב", "halls": 8},
    {"name": "Movieland Netanya", "name_he": "מובילנד נתניה", "city": "Netanya", "city_he": "נתניה", "halls": 6},
    {"name": "Movieland Haifa", "name_he": "מובילנד חיפה", "city": "Haifa", "city_he": "חיפה", "halls": 7},
    {"name": "Movieland Karmiel", "name_he": "מובילנד כרמיאל", "city": "Karmiel", "city_he": "כרמיאל", "halls": 5},
    {"name": "Movieland Afula", "name_he": "מובילנד עפולה", "city": "Afula", "city_he": "עפולה", "halls": 5},
]


def _seed_chain(db, chain_data: dict, branches: list[dict]):
    """Seed a single chain and its branches if not already present."""
    existing = db.query(CinemaChain).filter_by(name=chain_data["name"]).first()
    if existing:
        print(f"{chain_data['name']} already exists, skipping.")
        return

    chain = CinemaChain(**chain_data)
    db.add(chain)
    db.flush()

    for branch in branches:
        cinema = Cinema(
            chain_id=chain.id,
            name=branch["name"],
            name_he=branch["name_he"],
            city=branch["city"],
            city_he=branch["city_he"],
            halls_count=branch["halls"],
        )
        db.add(cinema)

    print(f"Seeded: {chain_data['name']} with {len(branches)} branches")


def seed_database():
    """Create all cinema chains and their branches."""
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()

    try:
        _seed_chain(db, HOT_CINEMA_CHAIN, HOT_CINEMA_BRANCHES)
        _seed_chain(db, MOVIELAND_CHAIN, MOVIELAND_BRANCHES)
        db.commit()
    except Exception as e:
        db.rollback()
        print(f"Seed error: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    seed_database()
