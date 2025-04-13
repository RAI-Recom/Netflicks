import pandas as pd
import pickle

def compute_popularity_model(movie_df, top_n=20):
    """
    Compute Bayesian average ranking based on rating + votes
    Returns top N popular movie_ids
    """
    movie_df = movie_df.dropna(subset=["rating", "votes"])

    C = movie_df["rating"].mean()
    m = movie_df["votes"].quantile(0.90)

    def bayesian_score(row):
        v = row["votes"]
        R = row["rating"]
        return (v / (v + m)) * R + (m / (v + m)) * C

    movie_df["popularity_score"] = movie_df.apply(bayesian_score, axis=1)
    top_movies = movie_df.sort_values("popularity_score", ascending=False)
    return top_movies["movie_id"].head(top_n).tolist()

def save_popularity_model(popular_ids, path="models/popular_movies.pkl"):
    # with open(path, "wb") as f:
    #     pickle.dump(popular_ids, f)
    print ("commented save- testing")

def train_and_save_popularity(movie_df, top_n=20):
    popular_ids = compute_popularity_model(movie_df, top_n)
    save_popularity_model(popular_ids)
    print(f"✅ Popularity model saved with top {top_n} movies.")
