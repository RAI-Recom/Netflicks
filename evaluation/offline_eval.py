# evaluation/offline_eval_hybrid.py

import os
import pandas as pd
import numpy as np
import pickle
from sklearn.metrics import mean_squared_error
from sklearn.metrics.pairwise import cosine_similarity
from db.db_manager import DBManager

# Paths (use mounted volume)
CF_MODEL_PATH = "/app/models/cf_model.pkl"
CB_USER_PROFILES = "/app/models/user_profiles.pkl"
CB_MOVIE_VECTORS = "/app/models/movie_vectors.pkl"
EVAL_OUTPUT_PATH = "/app/models/hybrid_metrics.json"

K = 30
ALPHA = 0.6
BETA = 0.4

# Load models
with open(CF_MODEL_PATH, "rb") as f:
    cf_model = pickle.load(f)

user_factors = cf_model["user_factors"]
item_factors = cf_model["item_factors"]

user_profiles = pd.read_pickle(CB_USER_PROFILES)
movie_vectors = pd.read_pickle(CB_MOVIE_VECTORS)

# Load test data from DB
db = DBManager()
#test_df = db.load_eval_ratings(mode="test", limit=10000)
#test_df = test_df.dropna(subset=["user_id", "movie_id", "rating"])

#user_id_to_idx = {uid: idx for idx, uid in enumerate(user_profiles.index)}
#movie_id_to_idx = {mid: idx for idx, mid in enumerate(movie_vectors.index)}
#test_df = test_df[test_df["user_id"].isin(user_id_to_idx) & test_df["movie_id"].isin(movie_id_to_idx)]

# Step 1: Get total count
total_rows = db.execute_query("SELECT COUNT(*) FROM ratings")[0][0]
eval_count = int(total_rows * 0.2)

# Step 2: Get last 20% based on most recent timestamps
test_df = db.execute_query(f"""
    SELECT r.user_id, r.movie_id, m.title AS movie_title, r.rating, r.updated_at
    FROM ratings r
    JOIN movies m ON r.movie_id = m.movie_id
    WHERE r.rating IS NOT NULL
    ORDER BY r.updated_at DESC
    LIMIT {eval_count}
""")
test_df = pd.DataFrame(test_df, columns=["user_id", "movie_id", "movie_title", "rating", "updated_at"])
test_df = test_df.dropna(subset=["user_id", "movie_id", "rating"])


def hybrid_predict(user_id, movie_id):
    try:
        u_idx = user_id_to_idx[user_id]
        m_idx = movie_id_to_idx[movie_id]
        cf_score = np.dot(user_factors[u_idx], item_factors[:, m_idx])
        cb_score_raw = cosine_similarity(
            [user_profiles.loc[user_id].values],
            [movie_vectors.loc[movie_id].values]
        )[0][0]
        cb_score_scaled = 1 + 4 * cb_score_raw
        return ALPHA * cf_score + BETA * cb_score_scaled
    except:
        return np.nan

test_df["hybrid_score"] = test_df.apply(lambda row: hybrid_predict(row["user_id"], row["movie_id"]), axis=1)

rmse = np.sqrt(mean_squared_error(test_df["rating"], test_df["hybrid_score"]))
print("✅ Hybrid RMSE:", round(rmse, 4))

# Compute HitRate@K
hit_count, total_users = 0, 0
user_groups = test_df.groupby("user_id")

for user_id, group in user_groups:
    if user_id not in user_id_to_idx:
        continue
    u_idx = user_id_to_idx[user_id]
    user_vec = user_profiles.loc[user_id].values
    scores = {}
    for movie_id in movie_vectors.index:
        try:
            m_idx = movie_id_to_idx[movie_id]
            cf_score = np.dot(user_factors[u_idx], item_factors[:, m_idx])
            cb_score_raw = cosine_similarity([user_vec], [movie_vectors.loc[movie_id].values])[0][0]
            cb_score_scaled = 1 + 4 * cb_score_raw
            scores[movie_id] = ALPHA * cf_score + BETA * cb_score_scaled
        except:
            continue
    top_k = sorted(scores, key=scores.get, reverse=True)[:K]
    if any(mid in top_k for mid in group["movie_id"].tolist()):
        hit_count += 1
    total_users += 1

hit_rate = hit_count / total_users if total_users else 0.0
print(f"✅ Hybrid HitRate@{K}:", round(hit_rate, 4))

# Save results
import json
with open(EVAL_OUTPUT_PATH, "w") as f:
    json.dump({
        "rmse": round(rmse, 4),
        f"hit_rate_at_{K}": round(hit_rate, 4)
    }, f, indent=2)
