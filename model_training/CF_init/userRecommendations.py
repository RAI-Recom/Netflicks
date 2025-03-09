import pandas as pd
import pickle
import numpy as np
from scipy.sparse import csr_matrix

ITEM_SIMILARITY_MODEL = "/home/Recomm-project/models/item_similarity.pkl"
RATINGS_FILE = "/home/Recomm-project/data/processed_rate_entries.csv"
USER_RECOMMENDATIONS_MODEL = "/home/Recomm-project/models/user_recommendations.pkl"

# Load the item similarity model
with open(ITEM_SIMILARITY_MODEL, "rb") as f:
    item_similarity_model = pickle.load(f)

# Read data in chunks to prevent memory overload
chunk_size = 500000  # Adjust based on memory availability
user_recommendations = {}
total_users_processed = 0

print("⚡ Processing user recommendations in batches...")
total_users = sum(1 for _ in open(RATINGS_FILE)) - 1

for chunk in pd.read_csv(RATINGS_FILE, names=["user_id", "movie_title", "rating"], chunksize=chunk_size, skiprows=1):
    
    # Convert user_id and movie_title to ordered categorical indices
    chunk["user_id"] = chunk["user_id"].astype("category").cat.as_ordered()
    chunk["movie_title"] = chunk["movie_title"].astype("category").cat.as_ordered()

    # Create a mapping for user and movie indices
    user_mapping = {user: idx for idx, user in enumerate(chunk["user_id"].cat.categories)}
    movie_mapping = {movie: idx for idx, movie in enumerate(chunk["movie_title"].cat.categories)}

    # Convert categorical user_id and movie_title into indices
    chunk["user_idx"] = chunk["user_id"].map(user_mapping)
    chunk["movie_idx"] = chunk["movie_title"].map(movie_mapping)

    # Build a sparse matrix
    ratings_sparse = csr_matrix((chunk["rating"], (chunk["user_idx"], chunk["movie_idx"])), dtype=np.float32)
    print(f"🔄 Processing users...")
    for user_id in chunk["user_id"].unique():
        user_idx = user_mapping[user_id]
        watched_movie_indices = ratings_sparse[user_idx].nonzero()[1]  # Get indices of watched movies

        watched_movies = [list(movie_mapping.keys())[list(movie_mapping.values()).index(idx)] for idx in watched_movie_indices]
        
        recommended_movies = {}

        for movie in watched_movies:
            if movie in item_similarity_model:
                similar_movies = sorted(
                    enumerate(item_similarity_model[movie]), key=lambda x: x[1], reverse=True
                )[1:11]  # Top 10 similar movies

                for sim_movie_idx, score in similar_movies:
                    sim_movie = list(movie_mapping.keys())[list(movie_mapping.values()).index(sim_movie_idx)]
                    if sim_movie not in watched_movies:
                        recommended_movies[sim_movie] = recommended_movies.get(sim_movie, 0) + score

        # Store recommendations or an empty list if none found
        user_recommendations[user_id] = sorted(recommended_movies, key=recommended_movies.get, reverse=True)[:20]
        # Track Progress
        total_users_processed += 1
        if total_users_processed % 1000 == 0:  # Print progress every 1000 users
            progress = (total_users_processed / total_users) * 100
            print(f"🔄 Processed {total_users_processed}/{total_users} users ({progress:.2f}% complete)")


# Save the precomputed recommendations
with open(USER_RECOMMENDATIONS_MODEL, "wb") as f:
    pickle.dump(user_recommendations, f)

print("✅ Precomputed user recommendations saved successfully!")
