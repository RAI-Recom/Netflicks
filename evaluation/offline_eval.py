# # evaluation/offline_eval_hybrid.py

# import os
# import pandas as pd
# import numpy as np
# import pickle
# import json
# from sklearn.metrics import mean_squared_error
# from sklearn.metrics.pairwise import cosine_similarity
# from db.db_manager import DBManager

# # Paths (use mounted volume)
# CF_MODEL_PATH      = "/app/models/cf_model.pkl"
# CB_USER_PROFILES   = "/app/models/user_profiles.pkl"
# CB_MOVIE_VECTORS   = "/app/models/movie_vectors.pkl"
# EVAL_OUTPUT_PATH   = "/app/models/hybrid_metrics.json"

# K     = 30
# ALPHA = 0.6
# BETA  = 0.4

# # 1) Load CF & CB models
# with open(CF_MODEL_PATH, "rb") as f:
#     cf_model = pickle.load(f)
# user_factors = cf_model["user_factors"]
# item_factors = cf_model["item_factors"]

# user_profiles  = pd.read_pickle(CB_USER_PROFILES)
# movie_vectors  = pd.read_pickle(CB_MOVIE_VECTORS)

# # 2) Build lookup maps
# user_id_to_idx  = {uid: idx for idx, uid in enumerate(user_profiles.index)}
# movie_id_to_idx = {mid: idx for idx, mid in enumerate(movie_vectors.index)}

# # 3) Load the last 20% of ratings
# db = DBManager()
# total_rows = db.execute_query("SELECT COUNT(*) FROM ratings")[0][0]
# eval_count = int(total_rows * 0.2)

# rows = db.execute_query(f"""
#     SELECT r.user_id, r.movie_id, m.title AS movie_title, r.rating, r.updated_at
#       FROM ratings r
#       JOIN movies m ON r.movie_id = m.movie_id
#      WHERE r.rating IS NOT NULL
#   ORDER BY r.updated_at DESC
#      LIMIT {eval_count}
# """)
# test_df = pd.DataFrame(rows, columns=["user_id","movie_id","movie_title","rating","updated_at"])

# # 4) Pre-filter to only those users/movies you can score
# mask_known = (
#     test_df["user_id"].isin(user_id_to_idx) &
#     test_df["movie_id"].isin(movie_id_to_idx)
# )
# filtered_out = (~mask_known).sum()
# test_df = test_df[mask_known].copy()
# print(f"Dropped {filtered_out} rows with unknown user/movie; {len(test_df)} remain")

# # 5) Define a clean predictor
# def hybrid_predict(user_id, movie_id):
#     u_idx = user_id_to_idx[user_id]
#     m_idx = movie_id_to_idx[movie_id]
#     cf_score = np.dot(user_factors[u_idx], item_factors[:, m_idx])
#     cb_raw   = cosine_similarity(
#         [user_profiles.loc[user_id].values],
#         [movie_vectors.loc[movie_id].values]
#     )[0][0]
#     cb_score = 1 + 4 * cb_raw
#     return ALPHA * cf_score + BETA * cb_score

# # 6) Compute hybrid scores
# test_df["hybrid_score"] = test_df.apply(
#     lambda r: hybrid_predict(r["user_id"], r["movie_id"]),
#     axis=1
# )

# # 7) Drop only the rows where the score still ended up NaN
# n_before = len(test_df)
# n_nan    = test_df["hybrid_score"].isna().sum()
# print(f"Found {n_nan}/{n_before} rows with NaN hybrid_score; dropping them")
# test_df = test_df.dropna(subset=["hybrid_score"])
# n_after  = len(test_df)
# print(f"Now evaluating on {n_after} rows")

# # 8) Compute RMSE
# rmse = np.sqrt(mean_squared_error(test_df["rating"], test_df["hybrid_score"]))
# print(f"✅ Hybrid RMSE: {rmse:.4f}")

# # 9) Compute HitRate@K
# hit_count, total_users = 0, 0
# for user_id, group in test_df.groupby("user_id"):
#     u_idx    = user_id_to_idx[user_id]
#     user_vec = user_profiles.loc[user_id].values

#     # score every movie in your CB index
#     scores = {}
#     for mid, m_idx in movie_id_to_idx.items():
#         cf = np.dot(user_factors[u_idx], item_factors[:, m_idx])
#         cb = 1 + 4 * cosine_similarity(
#                  [user_vec],
#                  [movie_vectors.loc[mid].values]
#              )[0][0]
#         scores[mid] = ALPHA * cf + BETA * cb

#     top_k = sorted(scores, key=scores.get, reverse=True)[:K]
#     if any(m in top_k for m in group["movie_id"]):
#         hit_count += 1
#     total_users += 1

# hit_rate = hit_count / total_users if total_users else 0.0
# print(f"✅ Hybrid HitRate@{K}: {hit_rate:.4f}")

# # 10) Save results
# with open(EVAL_OUTPUT_PATH, "w") as out:
#     json.dump({
#         "rmse": round(rmse, 4),
#         f"hit_rate_at_{K}": round(hit_rate, 4)
#     }, out, indent=2)



import pandas as pd
import numpy as np
import pickle
from sklearn.metrics import mean_squared_error
from tqdm import tqdm
from db.db_manager import DBManager
from pipeline.hybrid_recommend import hybrid_recommend, cf_model, movie_id_to_title

# Constants
K = 40  # For Hit Rate@K
TEST_SIZE_RATIO = 0.2  # Last 20% of ratings for test
DEFAULT_PRED_RATING = 3.0  # fallback

# Load all ratings
db_manager = DBManager()
# Step 1: Count total ratings
n_total = db_manager.count_ratings()

# Step 2: Calculate test size
n_test = int(n_total * TEST_SIZE_RATIO)

# Step 3: Fetch last 20% ratings only
ratings_df = db_manager.load_ratings_chunk(limit=n_test, offset=n_total - n_test)

# Step 4: Prepare
ratings_df = ratings_df.dropna(subset=["rating"])

#test_ratings = ratings_df.iloc[-n_test:]

# Evaluation metrics
actual_ratings = []
predicted_ratings = []
hits = 0
total_users = 0

# Group test ratings by user
user_groups = ratings_df.groupby('user_id')


for user_id, group in tqdm(user_groups, desc="Evaluating"):
    if len(group) < 5:
        continue  # skip users with too few ratings

    # Also: Skip if user_id not in training user list (optional, stricter)
    if user_id not in user_profiles.index:
        continue
    
    true_movies = group['movie_id'].tolist()
    true_ratings = group['rating'].tolist()
    
    # Get top-K recommendations for this user
    recommended_titles = hybrid_recommend(user_id, top_n=K)
    
    # Ground-truth titles
    true_movie_titles = [movie_id_to_title.get(mid, f"Unknown Title ({mid})") for mid in true_movies]
    
    # Check if any ground-truth movie is in top-K recommendations
    user_hit = any(title in recommended_titles for title in true_movie_titles)
    if user_hit:
        hits += 1
    total_users += 1
    
    # RMSE computation
    for movie_id, true_rating in zip(true_movies, true_ratings):
        try:
            pred = cf_model.predict(str(user_id), str(movie_id)).est
        except Exception:
            pred = DEFAULT_PRED_RATING  # fallback if prediction fails
        actual_ratings.append(true_rating)
        predicted_ratings.append(pred)

# Final metrics
rmse = np.sqrt(mean_squared_error(actual_ratings, predicted_ratings))
hitrate = hits / total_users if total_users > 0 else 0.0

# Output
print("\n=== Offline Evaluation ===")
print(f"RMSE: {rmse:.4f}")
print(f"Hit Rate@{K}: {hitrate:.4f}")
