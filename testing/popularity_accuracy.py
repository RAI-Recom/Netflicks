import pandas as pd
import numpy as np
import pickle

# -------------------------------
# 1. Load the Pre-trained Popularity Model
# -------------------------------

POPULARITY_MODEL = "/home/Recomm-project/models/popular_movies.pkl"

# Load the popularity-based model (a sorted list of (movie_title, score) tuples)
with open(POPULARITY_MODEL, "rb") as f:
    popular_movies = pickle.load(f)

# Number of recommendations to consider per user
K = 10

# Extract the top K recommended movies from the model.
top_K_movies = [movie for movie, score in popular_movies][:K]
print("Top K Movies:", top_K_movies)

# -------------------------------
# 2. Load the Ratings Data for Evaluation
# -------------------------------
# Assuming the ratings file has the following columns: user_id, movie_title, rating.
RATINGS_FILE = "/home/Recomm-project/data/processed_rate_entries.csv"

# If the CSV does not have headers, we explicitly assign them.
ratings = pd.read_csv(RATINGS_FILE, names=["user_id", "movie_title", "rating"], skiprows=1)
print("Ratings Data Sample:")
print(ratings.head())

# -------------------------------
# 3. Prepare Evaluation Data
# -------------------------------
# Build a dictionary mapping each user to the set of movies they have interacted with.
user_interactions = ratings.groupby("user_id")["movie_title"].apply(set).to_dict()

# -------------------------------
# 4. Evaluate the Recommendations
# -------------------------------
# For a popularity-based model, every user gets the same top-K recommendation list.
# We'll compute:
# - Hit Rate: Fraction of users for whom at least one recommended movie is in their interaction set.
# - Precision@K: For each user, the fraction of recommended movies that appear in their interaction set.

hit_counts = []       # List to record a "hit" (1) if the user gets at least one relevant recommendation.
precision_scores = [] # List to record Precision@K for each user.

# Evaluate each user's interactions against the top K recommendations.
for user, true_movies in user_interactions.items():
    hits = len(set(top_K_movies) & true_movies)
    hit_counts.append(1 if hits > 0 else 0)
    precision_scores.append(hits / K)

# Calculate overall metrics.
hit_rate = np.mean(hit_counts)
precision_at_K = np.mean(precision_scores)

print(f"\nEvaluation Metrics:")
print(f"Hit Rate: {hit_rate:.4f}")
print(f"Precision@{K}: {precision_at_K:.4f}")
