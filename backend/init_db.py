"""Database initialization module.

This module handles the creation of database tables and initial setup.
It should be run once before starting the application.
"""

from sqlalchemy import create_engine

from config import DATABASE_URL
from models import Base


def init_database():
    """Initialize the database by creating all tables.

    Creates all tables defined in the models module using SQLAlchemy's
    create_all() method.
    """
    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)


if __name__ == "__main__":
    init_database()
