# import pandas as pd
# import numpy as np
# import pickle
# from sklearn.metrics import mean_squared_error
# from tqdm import tqdm
# from db.db_manager import DBManager
# from pipeline.hybrid_recommend import hybrid_recommend, cf_model, movie_id_to_title, user_profiles

# # Constants
# K = 40  # For Hit Rate@K
# TEST_SIZE_RATIO = 0.2  # Last 20% of ratings for test
# DEFAULT_PRED_RATING = 3.0  # fallback

# # Load all ratings
# db_manager = DBManager()
# # Step 1: Count total ratings
# n_total = db_manager.count_ratings()

# # Step 2: Calculate test size
# n_test = int(n_total * TEST_SIZE_RATIO)

# # Step 3: Fetch last 20% ratings only
# ratings_df = db_manager.load_ratings_chunk(limit=n_test, offset=n_total - n_test)

# # Step 4: Prepare
# ratings_df = ratings_df.dropna(subset=["rating"])

# #test_ratings = ratings_df.iloc[-n_test:]

# # Evaluation metrics
# actual_ratings = []
# predicted_ratings = []
# hits = 0
# total_users = 0

# # Group test ratings by user
# user_groups = ratings_df.groupby('user_id')


# for user_id, group in tqdm(user_groups, desc="Evaluating"):
#     '''Uncomment after training is done iteratively
#     if len(group) < 5:
#         continue  # skip users with too few ratings

#     # Also: Skip if user_id not in training user list (optional, stricter)
#     if user_id not in user_profiles.index:
#         continue
#     '''
    
#     true_movies = group['movie_id'].tolist()
#     true_ratings = group['rating'].tolist()
    
#     # Get top-K recommendations for this user
#     recommended_titles = hybrid_recommend(user_id, top_n=K)
    
#     # Ground-truth titles
#     true_movie_titles = [movie_id_to_title.get(mid, f"Unknown Title ({mid})") for mid in true_movies]
    
#     # Check if any ground-truth movie is in top-K recommendations
#     user_hit = any(title in recommended_titles for title in true_movie_titles)
#     if user_hit:
#         hits += 1
#     total_users += 1
    
#     # RMSE computation
#     for movie_id, true_rating in zip(true_movies, true_ratings):
#         try:
#             pred = cf_model.predict(str(user_id), str(movie_id)).est
#         except Exception:
#             pred = DEFAULT_PRED_RATING  # fallback if prediction fails
#         actual_ratings.append(true_rating)
#         predicted_ratings.append(pred)

# # Final metrics
# rmse = np.sqrt(mean_squared_error(actual_ratings, predicted_ratings))
# hitrate = hits / total_users if total_users > 0 else 0.0

# # Output
# print("\n=== Offline Evaluation ===")
# print(f"RMSE: {rmse:.4f}")
# print(f"Hit Rate@{K}: {hitrate:.4f}")


'''
WATCH DATA IS ALSO USED FOR HIT RATE FROM HERE
'''

import pandas as pd
import numpy as np
import pickle
from sklearn.metrics import mean_squared_error
from tqdm import tqdm
from db.db_manager import DBManager
from pipeline.hybrid_recommend import hybrid_recommend, cf_model, movie_id_to_title, user_profiles

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
test_user_ids = ratings_df['user_id'].unique().tolist()
watch_df = db_manager.load_watch_for_users(test_user_ids)

merged_df = pd.merge(
    ratings_df,
    watch_df[["user_id", "movie_id", "watched_minutes"]],
    on=["user_id", "movie_id"],
    how="left"  # keep all ratings, add watch time if available
)


# Evaluation metrics
actual_ratings = []
predicted_ratings = []
hits = 0
total_users = 0

# Group test ratings by user
#user_groups = ratings_df.groupby('user_id')
WATCH_TIME_THRESHOLD = 5  # minutes

user_groups = merged_df.groupby('user_id')


for user_id, group in tqdm(user_groups, desc="Evaluating"):
    '''Uncomment after training is done iteratively
    if len(group) < 5:
        continue  # skip users with too few ratings

    # Also: Skip if user_id not in training user list (optional, stricter)
    if user_id not in user_profiles.index:
        continue
    '''
    
    true_movies = group['movie_id'].tolist()
    true_ratings = group['rating'].tolist()
    
    # Get top-K recommendations for this user
    recommended_titles = hybrid_recommend(user_id, top_n=K)
    # Only consider movies with enough watch time
   
    watched_movies = group[group["watched_minutes"] >= WATCH_TIME_THRESHOLD]["movie_id"].tolist()

    # Ground-truth titles
    true_movie_titles = [movie_id_to_title.get(mid, f"Unknown Title ({mid})") for mid in watched_movies]
    
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





