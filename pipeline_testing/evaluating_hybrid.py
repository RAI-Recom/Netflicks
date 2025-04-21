import pandas as pd
import numpy as np
import os
import json
import pickle
from sklearn.metrics import mean_squared_error
from sklearn.metrics.pairwise import cosine_similarity
from pipeline import load_data

# --- CONFIG ---
CUTOFF_DATE = "2025-03-01 00:00:00"
CF_MODEL_PATH = "evaluation/cf_model.pkl"
CB_USER_PROFILES = "evaluation/user_profiles.pkl"
CB_MOVIE_VECTORS = "evaluation/movie_vectors.pkl"
EVAL_OUTPUT_PATH = "evaluation/hybrid_metrics1.json"
K = 30
ALPHA = 0.6  # CF weight
BETA = 0.4   # CB weight

# --- LOAD CF MODEL ---
with open(CF_MODEL_PATH, "rb") as f:
    cf_model = pickle.load(f)

user_factors = cf_model["user_factors"]
item_factors = cf_model["item_factors"]

# --- LOAD CB PROFILES ---
user_profiles = pd.read_pickle(CB_USER_PROFILES)
movie_vectors = pd.read_pickle(CB_MOVIE_VECTORS)

# --- LOAD TEST DATA ---
test_df = load_data.load_explicit_ratings(cutoff=CUTOFF_DATE, mode="test", limit=10000)
test_df = test_df.dropna(subset=["user_id", "movie_id", "rating"])

# --- Create index mappings ---
user_id_to_idx = {uid: idx for idx, uid in enumerate(user_profiles.index)}
movie_id_to_idx = {mid: idx for idx, mid in enumerate(movie_vectors.index)}

# --- Filter only valid users and movies ---
test_df = test_df[test_df["user_id"].isin(user_id_to_idx) & test_df["movie_id"].isin(movie_id_to_idx)]

# --- Predict hybrid score ---
def hybrid_predict(user_id, movie_id):
    try:
        u_idx = user_id_to_idx[user_id]
        m_idx = movie_id_to_idx[movie_id]

        # CF score (dot product)
        cf_score = np.dot(user_factors[u_idx], item_factors[:, m_idx])

        # CB score (cosine similarity)
        cb_score_raw = cosine_similarity(
            [user_profiles.loc[user_id].values],
            [movie_vectors.loc[movie_id].values]
        )[0][0]

        # Scale CB score to match 1–5 rating range
        cb_score_scaled = 1 + 4 * cb_score_raw  # from [0, 1] → [1, 5]

        # Hybrid score (weighted)
        return ALPHA * cf_score + BETA * cb_score_scaled
    except:
        return np.nan

test_df['hybrid_score'] = test_df.apply(lambda row: hybrid_predict(row["user_id"], row["movie_id"]), axis=1)

# --- Compute RMSE ---
rmse = np.sqrt(mean_squared_error(test_df["rating"], test_df["hybrid_score"]))
print("Hybrid RMSE (with CB scaling):", round(rmse, 4))

# --- Compute HitRate@K ---
hit_count = 0
total_users = 0
user_groups = test_df.groupby("user_id")

for user_id, group in user_groups:
    if user_id not in user_id_to_idx:
        continue
    u_idx = user_id_to_idx[user_id]
    user_vec = user_profiles.loc[user_id].values

    # Score all movies
    scores = {}
    for movie_id in movie_vectors.index:
        try:
            m_idx = movie_id_to_idx[movie_id]
            cf_score = np.dot(user_factors[u_idx], item_factors[:, m_idx])
            cb_score_raw = cosine_similarity([user_vec], [movie_vectors.loc[movie_id].values])[0][0]
            cb_score_scaled = 1 + 4 * cb_score_raw
            hybrid_score = ALPHA * cf_score + BETA * cb_score_scaled
            scores[movie_id] = hybrid_score
        except:
            continue

    top_k = sorted(scores, key=scores.get, reverse=True)[:K]
    actual_movies = group["movie_id"].tolist()

    if any(mid in top_k for mid in actual_movies):
        hit_count += 1
    total_users += 1

hit_rate = hit_count / total_users if total_users > 0 else 0.0
print(f"Hybrid HitRate@{K}:", round(hit_rate, 4))

# --- Save metrics ---
os.makedirs(os.path.dirname(EVAL_OUTPUT_PATH), exist_ok=True)
with open(EVAL_OUTPUT_PATH, "w") as f:
    json.dump({
        "cutoff": CUTOFF_DATE,
        "rmse": rmse,
        f"hit_rate_at_{K}": hit_rate,
        "alpha": ALPHA,
        "beta": BETA,
        "cb_scaling_applied": True
    }, f, indent=2)

print(f"Hybrid metrics saved to {EVAL_OUTPUT_PATH}")
