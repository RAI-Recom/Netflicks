import pandas as pd

def build_movie_genre_vectors(watch_df, genre_cols):
    """
    Creates a movie feature matrix using genre columns.
    """
    movie_df = watch_df.drop_duplicates("movie_id")[["movie_id", "genres"]]
    movie_df["genres"] = movie_df["genres"].apply(lambda x: x.split() if isinstance(x, str) else [])

    for col in genre_cols:
        genre = col.replace("genre_", "").replace("_", " ").title()
        movie_df[col] = movie_df["genres"].apply(lambda x: int(genre in x))

    movie_vectors = movie_df.set_index("movie_id")[genre_cols]
    return movie_vectors

def save_profiles(user_profiles, movie_vectors, user_path="models/user_profiles.pkl", movie_path="models/movie_vectors.pkl"):
    """
    Saves user and movie profiles to disk.
    """
    user_profiles.to_pickle(user_path)
    movie_vectors.to_pickle(movie_path)
