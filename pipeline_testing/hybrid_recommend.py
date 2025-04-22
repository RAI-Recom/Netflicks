import pandas as pd
import numpy as np
import pickle
from sklearn.metrics.pairwise import cosine_similarity
import json
from sqlalchemy import create_engine
from pipeline_testing.model_pipelines import load_data

# Load models
cf_model = pickle.load(open("models/cf_model.pkl", "rb"))
user_profiles = pd.read_pickle("models/user_profiles.pkl")
movie_vectors = pd.read_pickle("models/movie_vectors.pkl")
top_popular_movies = pickle.load(open("models/popular_movies.pkl", "rb"))

# --- Utility to get SQLAlchemy engine ---
def get_sqlalchemy_engine():
    db = load_data.load_config()
    db_uri = f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['dbname']}"
    return create_engine(db_uri)

# --- Utility to fetch mapping from SQL ---
def fetch_movie_title_map():
    engine = get_sqlalchemy_engine()
    query = "SELECT movie_id, movie_title_id FROM movies;"
    df = pd.read_sql(query, engine)
    return dict(zip(df["movie_id"],df["movie_title_id"]))

# --- Fetch once and reuse ---
movie_id_to_title = fetch_movie_title_map()

def get_weights(user_id):
    try:
        n_rated = cf_model["user_rating_count"].get(user_id, 0)
    except KeyError:
        return 0.0, 1.0  # fallback to content only

    if n_rated >= 10:
        return 0.8, 0.2
    elif n_rated > 0:
        return 0.5, 0.5
    else:
        return 0.0, 1.0  # fallback to CB

def hybrid_recommend(user_id, top_n=20):
    alpha, beta = get_weights(user_id)

    cb_scores = {}
    cf_scores = {}

    all_movie_ids = movie_vectors.index.tolist()

    # === Content-based ===
    if user_id in user_profiles.index:
        user_vec = user_profiles.loc[[user_id]]
        cos_sim = cosine_similarity(user_vec, movie_vectors)[0]
        cb_scores = dict(zip(movie_vectors.index, cos_sim))

    # === Collaborative filtering ===
    try:
        for movie_id in all_movie_ids:
            pred = cf_model.predict(str(user_id), str(movie_id)).est
            cf_scores[movie_id] = pred
    except Exception:
        cf_scores = {}

    # === Combine scores ===
    final_scores = {}
    for movie_id in all_movie_ids:
        cf_score = cf_scores.get(movie_id, 0)
        cb_score = cb_scores.get(movie_id, 0)
        score = alpha * cf_score + beta * cb_score
        if score > 0:
            final_scores[movie_id] = score

    if final_scores:
        ranked = sorted(final_scores.items(), key=lambda x: x[1], reverse=True)
        return [movie_id_to_title.get(movie_id, f"Unknown Title ({movie_id})") for movie_id, _ in ranked[:top_n]]
    else:
        return [movie_id_to_title.get(mid, f"Unknown Title ({mid})") for mid in top_popular_movies[:top_n]]
