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
                'user': os.getenv('DB_USER'),
                'password': os.getenv('DB_PASSWORD'),
                'dbname': os.getenv('DB_NAME'),
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

def load_eval_ratings(cutoff="2025-03-01 00:00:00", mode="train", limit=10000, offset=0):
    engine = get_sqlalchemy_engine()

    # Decide operator based on mode
    if mode == "train":
        time_condition = f"r.updated_at <= '{cutoff}'"
    elif mode == "test":
        time_condition = f"r.updated_at > '{cutoff}'"
    else:
        raise ValueError("Mode must be either 'train' or 'test'")

    query = f"""
        SELECT r.user_id, r.movie_id, m.title AS movie_title, r.rating, r.updated_at
        FROM ratings r
        JOIN movies m ON r.movie_id = m.movie_id
        WHERE r.rating IS NOT NULL AND {time_condition}
        ORDER BY r.updated_at
        LIMIT {limit} OFFSET {offset};
    """
    return pd.read_sql(query, con=engine)

def load_eval_watch(cutoff="2025-03-01 00:00:00", mode="train", limit=10000, offset=0):
    engine = get_sqlalchemy_engine()

    # Decide operator based on mode
    if mode == "train":
        time_condition = f"r.updated_at <= '{cutoff}'"
    elif mode == "test":
        time_condition = f"r.updated_at > '{cutoff}'"
    else:
        raise ValueError("Mode must be either 'train' or 'test'")

    query = f"""
        SELECT w.user_id, w.movie_id, w.watched_minutes, m.title, m.genres, m.plot
        FROM watch_history w
        JOIN movies m ON w.movie_id = m.movie_id
        WHERE w.watched_minutes IS NOT NULL AND {time_condition}
        ORDER BY w.updated_at
        LIMIT {limit} OFFSET {offset};
    """
    return pd.read_sql(query, con=engine)