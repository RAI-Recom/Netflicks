import pandas as pd
import pickle
import numpy as np
from scipy.sparse import csr_matrix
from sklearn.metrics.pairwise import cosine_similarity

ITEM_SIMILARITY_MODEL = "/home/Recomm-project/models/item_similarity.pkl"
RATINGS_FILE = "/home/Recomm-project/data/processed_rate_entries.csv"

def load_data():
    """ Load data in chunks to handle large files """
    chunk_size = 500000  # Adjust based on memory availability
    ratings_list = []

    for chunk in pd.read_csv(RATINGS_FILE, names=["user_id", "movie_title", "rating"], chunksize=chunk_size, skiprows=1):
        ratings_list.append(chunk)

    return pd.concat(ratings_list, ignore_index=True)

def train_item_similarity():
    """ Compute item-item similarity in batches to prevent memory overload """
    df = load_data()

    # Reduce dataset size by filtering out movies with very few ratings (adjust threshold if needed)
    min_ratings = 7
    df = df[df.groupby("movie_title")["rating"].transform("count") >= min_ratings]

    # Create a mapping of movie titles to indices
    movie_mapping = {movie: idx for idx, movie in enumerate(df["movie_title"].unique())}
    movie_reverse_mapping = {idx: movie for movie, idx in movie_mapping.items()}  # Reverse lookup

    # Create user-movie matrix in sparse format
    user_ids = df["user_id"].astype("category").cat.codes
    movie_ids = df["movie_title"].map(movie_mapping)

    # Build a sparse matrix
    ratings_sparse = csr_matrix((df["rating"], (user_ids, movie_ids)), dtype=np.float32)

    # Compute item similarity in small batches
    batch_size = 1000  # Compute similarity for 1000 movies at a time
    total_movies = ratings_sparse.shape[1]
    movie_sim_dict = {}

    print(f"⚡ Computing similarity for {total_movies} movies in batches...")

    for start in range(0, total_movies, batch_size):
        end = min(start + batch_size, total_movies)
        print(f"Processing movies {start} to {end}...")

        # Compute similarity only for this batch of movies
        batch_similarity = cosine_similarity(ratings_sparse[:, start:end].T, ratings_sparse.T, dense_output=False)

        # Convert sparse similarity to a dictionary for fast lookup
        for i, movie_idx in enumerate(range(start, end)):
            movie_sim_dict[movie_reverse_mapping[movie_idx]] = batch_similarity[i].toarray().flatten()

    # Save model
    with open(ITEM_SIMILARITY_MODEL, "wb") as f:
        pickle.dump(movie_sim_dict, f)

    print(f"Item-Item similarity model saved successfully at {ITEM_SIMILARITY_MODEL}")

if __name__ == "__main__":
    train_item_similarity()
