import pandas as pd
import pickle

ratings_file = "/home/Recomm-project/data/processed_rate_entries.csv"
popularity_file = "/home/Recomm-project/model/popularityRanking/popular_movies.pkl"

chunk_size = 500000  # Process data in chunks
movie_ratings = {}

# Read in chunks to handle large data
for chunk in pd.read_csv(ratings_file, names=["user_id", "movie_title", "rating"], chunksize=chunk_size, skiprows=1):
    for movie, rating in zip(chunk["movie_title"], chunk["rating"]):
        if movie not in movie_ratings:
            movie_ratings[movie] = []
        movie_ratings[movie].append(rating)

# Compute popularity scores
def compute_popularity_scores(movie_ratings, m=50):
    all_ratings = [int(rating) for ratings in movie_ratings.values() for rating in ratings]
    C = sum(all_ratings) / len(all_ratings)  # Global average rating
    
    scores = {}
    for movie, ratings in movie_ratings.items():
        v = len(ratings)
        R = sum(ratings) / v
        scores[movie] = (v / (v + m)) * R + (m / (v + m)) * C
    
    return sorted(scores.items(), key=lambda x: x[1], reverse=True)

popular_movies = compute_popularity_scores(movie_ratings)

# Save computed scores
with open(popularity_file, "wb") as f:
    pickle.dump(popular_movies, f)

print(f"Processed and stored popular movies in {popularity_file}")
