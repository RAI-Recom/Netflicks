import pandas as pd
import numpy as np
import pickle

# -----------------------------------------------------
# 1. Load the Pre-trained User Recommendations
# -----------------------------------------------------
USER_RECOMMENDATIONS_MODEL = "/home/Recomm-project/models/user_recommendations.pkl"
with open(USER_RECOMMENDATIONS_MODEL, "rb") as f:
    user_recommendations = pickle.load(f)

# Convert each user's recommendations to a set for faster membership checks
# Also note how many recommendations we gave each user (assuming you gave them K, or up to K).
recommended_sets = {}
for user, recs in user_recommendations.items():
    recommended_sets[user] = set(recs)  # or recs[:K], if your stored recs are more than K

num_users = len(recommended_sets)
print(f"Loaded recommendations for {num_users} users.")

# We'll assume each user got exactly K recommendations (if they had that many)
# If it varies by user, you can store that or handle it in the loop below.
K = 10  # or however many you recommended per user

# -----------------------------------------------------
# 2. Prepare Data Structures for Incremental Evaluation
# -----------------------------------------------------
# We'll track how many recommended movies each user actually "hit" (found in the dataset).
user_hits = {}
user_has_hit = {}

# Initialize to 0 hits and False for each user
for user in recommended_sets:
    user_hits[user] = 0
    user_has_hit[user] = False

# -----------------------------------------------------
# 3. Chunked Reading of the Large CSV
# -----------------------------------------------------
PROCESSED_DATA_FILE = "/home/Recomm-project/data/processed_data_entries.csv"  # 40GB file
CHUNK_SIZE = 500000  # Adjust based on your memory constraints

# Optional: define a watch threshold in minutes
WATCHED_THRESHOLD = 1  # or 30, or any other cutoff

# CSV has columns: timestamp, user_id, movie_title, watched_minutes
# If there's no header row, you can specify `names=[...]` and `skiprows=1`
reader = pd.read_csv(
    PROCESSED_DATA_FILE,
    usecols=["user_id", "movie_title", "watched_minutes"],  # Only load needed columns
    chunksize=CHUNK_SIZE
)

print("Starting chunked processing for hit rate calculation...")

chunk_index = 0
for chunk in reader:
    chunk_index += 1
    print(f"Processing chunk #{chunk_index} with {len(chunk)} rows...")

    # For each row in this chunk, check if there's a hit
    for _, row in chunk.iterrows():
        user = row["user_id"]
        movie = row["movie_title"]
        minutes = row["watched_minutes"]

        # Check if user is in our recommendations
        if user in recommended_sets:
            # Check threshold
            if minutes >= WATCHED_THRESHOLD:
                # Check if recommended
                if movie in recommended_sets[user]:
                    user_hits[user] += 1
                    user_has_hit[user] = True

    # You could optionally delete the chunk variable or call gc.collect() 
    # if you need to be extra careful about memory usage.
    del chunk

print("Finished reading all chunks.")

# -----------------------------------------------------
# 4. Compute Hit Rate & Precision@K
# -----------------------------------------------------
# Hit Rate = fraction of users who had at least one hit
hit_rate = np.mean([1 if user_has_hit[u] else 0 for u in recommended_sets])

# Precision@K = total hits / (K * number_of_users_who_received_recs)
# If some users got fewer than K recs, you can adjust accordingly.
total_hits = sum(user_hits[u] for u in recommended_sets)
precision_at_k = total_hits / (K * num_users)

print("\nEvaluation Results (Chunked Approach):")
print(f"Hit Rate: {hit_rate:.4f}")
print(f"Precision@{K}: {precision_at_k:.4f}")
