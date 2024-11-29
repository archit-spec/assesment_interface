import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from models import Base, engine
from config import DATABASE_URL

def init_database():
    """Initialize the database by creating all tables"""
    try:
        # Connect to postgres database first
        conn = psycopg2.connect(
            user="dumball",
            password="tammu123",
            host="localhost",
            port="5432",
            database="postgres"  # Connect to postgres database first
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        # Check if database exists
        cur.execute("SELECT 1 FROM pg_database WHERE datname='accha'")
        exists = cur.fetchone()
        
        if not exists:
            cur.execute('CREATE DATABASE accha')
            print("Database 'accha' created successfully!")
        else:
            print("Database 'accha' already exists")
        
        cur.close()
        conn.close()

        # Create tables
        Base.metadata.create_all(bind=engine)
        print("Database tables created successfully!")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    init_database()
