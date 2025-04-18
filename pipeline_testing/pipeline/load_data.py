from sqlalchemy import create_engine
import pandas as pd
import json
import os
from dotenv import load_dotenv


def load_config():
    load_dotenv()
    return {
                'host': os.getenv('HOST'),
                'port': os.getenv('DB_PORT'),
                'user': os.getenv('DB_USER'),  # Fetch username from .env
                'password': os.getenv('DB_PASSWORD'),  # Fetch password from .env
                'dbname': os.getenv('DB_NAME'),  # Corrected to fetch the database name
            }

def get_sqlalchemy_engine():
    db = load_config()
    db_uri = f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['dbname']}"
    print(db_uri)
    return create_engine(db_uri)

def load_ratings_chunk(limit=10000, offset=0):
    engine = get_sqlalchemy_engine()
    query = f"""
        SELECT r.user_id, r.movie_id, r.rating, m.title, m.genres, m.plot
        FROM ratings r
        JOIN movies m ON r.movie_id = m.movie_id
        ORDER BY r.updated_at
        LIMIT {limit} OFFSET {offset};
    """
    return pd.read_sql(query, con=engine)

def load_watch_chunk(limit=10000, offset=0):
    engine = get_sqlalchemy_engine()
    query = f"""
        SELECT w.user_id, w.movie_id, w.watched_minutes, m.title, m.genres, m.plot
        FROM watch_history w
        JOIN movies m ON w.movie_id = m.movie_id
        ORDER BY w.updated_at
        LIMIT {limit} OFFSET {offset};
    """
    return pd.read_sql(query, con=engine)

def load_movies(limit=1000):
    engine = get_sqlalchemy_engine()
    query = """
        SELECT movie_id, title, rating, votes, genres
        FROM movies
        WHERE rating IS NOT NULL AND votes IS NOT NULL
    """
    if limit:
        query += f" LIMIT {limit}"
    return pd.read_sql(query, con=engine)