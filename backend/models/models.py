from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
from datetime import datetime

from database import Base


class CinemaChain(Base):
    """רשת קולנוע (Yes Planet, Cinema City, Hot Cinema, וכו')"""
    __tablename__ = "cinema_chains"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, nullable=False)
    name_he = Column(String)
    website = Column(String)
    logo_url = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

    cinemas = relationship("Cinema", back_populates="chain")


class Cinema(Base):
    """סניף קולנוע ספציפי"""
    __tablename__ = "cinemas"

    id = Column(Integer, primary_key=True, index=True)
    chain_id = Column(Integer, ForeignKey("cinema_chains.id"), nullable=False)
    name = Column(String, nullable=False)
    name_he = Column(String)
    city = Column(String, nullable=False)
    city_he = Column(String)
    address = Column(String)
    halls_count = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)

    chain = relationship("CinemaChain", back_populates="cinemas")
    screenings = relationship("Screening", back_populates="cinema")


class Movie(Base):
    """סרט"""
    __tablename__ = "movies"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, nullable=False)
    title_he = Column(String)
    genre = Column(String)
    duration_minutes = Column(Integer)
    release_date = Column(String)
    poster_url = Column(String)
    rating = Column(String)
    director = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

    screenings = relationship("Screening", back_populates="movie")

    __table_args__ = (
        UniqueConstraint("title", "release_date", name="uq_movie_title_release"),
    )


class Screening(Base):
    """הקרנה ספציפית"""
    __tablename__ = "screenings"

    id = Column(Integer, primary_key=True, index=True)
    movie_id = Column(Integer, ForeignKey("movies.id"), nullable=False)
    cinema_id = Column(Integer, ForeignKey("cinemas.id"), nullable=False)
    showtime = Column(DateTime, nullable=False)
    hall = Column(String)
    format = Column(String)  # 2D, 3D, IMAX, 4DX, ScreenX
    language = Column(String)  # dubbed, subtitled, original
    ticket_price = Column(Float)
    tickets_sold = Column(Integer, default=0)
    total_seats = Column(Integer)
    revenue = Column(Float, default=0.0)
    status = Column(String, default="active")  # active, closed
    scraped_at = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)

    movie = relationship("Movie", back_populates="screenings")
    cinema = relationship("Cinema", back_populates="screenings")


class ScrapeLog(Base):
    """לוג של סריקות"""
    __tablename__ = "scrape_logs"

    id = Column(Integer, primary_key=True, index=True)
    chain_name = Column(String, nullable=False)
    status = Column(String, nullable=False)  # success, error
    movies_found = Column(Integer, default=0)
    screenings_found = Column(Integer, default=0)
    error_message = Column(String)
    duration_seconds = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)
