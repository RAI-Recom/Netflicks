import pandas as pd
import numpy as np
import pickle

# Load dataset
df = pd.read_csv("data/watch_data_combined.csv", index_col=0)
df = df.dropna(subset=["genres"])

# Extract unique genres
all_genres = sorted(set(genre for sublist in df["genres"].str.split(", ") for genre in sublist))

def encode_genres(movie_genres):
    return np.array([1 if genre in movie_genres else 0 for genre in all_genres])

df["genre_vector"] = df["genres"].apply(encode_genres)

def get_watch_weight(watched_minutes):
    if pd.isna(watched_minutes):
        watched_minutes = 0
    if watched_minutes >= 80:
        return 1.0
    elif watched_minutes >= 40:
        return 0.5
    elif watched_minutes <= 20:
        return -0.5
    else:
        return 0

df["watch_weight"] = df["watched_minutes"].apply(get_watch_weight)

user_profiles = {}
movie_profiles = {}

for user_id, group in df.groupby("user_id"):
    profile = np.zeros(len(all_genres))
    weights = group["watch_weight"].values
    vectors = np.vstack(group["genre_vector"].values)
    profile = np.sum(weights[:, np.newaxis] * vectors, axis=0)
    if np.linalg.norm(profile) != 0:
        profile = profile / np.linalg.norm(profile)
    user_profiles[user_id] = profile

for movie_id, group in df.groupby("movie_id"):
    movie_profiles[movie_id] = group.iloc[0]["genre_vector"]

# Save profiles
with open("models/user_profiles.pkl", "wb") as f:
    pickle.dump(user_profiles, f)

with open("models/movie_profiles.pkl", "wb") as f:
    pickle.dump(movie_profiles, f)

with open("models/genre_list.pkl", "wb") as f:
    pickle.dump(all_genres, f)

print("Content Profiles saved.")
