from sklearn.metrics.pairwise import cosine_similarity
import pandas as pd

def assign_watch_weight(minutes):
    if pd.isna(minutes): return 0.0
    if minutes >= 80: return 1.0
    elif minutes >= 40: return 0.5
    elif minutes <= 20: return -0.5
    return 0.0

def build_user_genre_profiles(df):
    df["genres"] = df["genres"].apply(lambda x: x.split() if isinstance(x, str) else [])

    all_genres = set(g for genres in df["genres"] for g in genres)
    genre_cols = []

    for genre in all_genres:
        col = f"genre_{genre.strip().replace(' ', '_').lower()}"
        df[col] = df["genres"].apply(lambda x: int(genre in x))
        genre_cols.append(col)

    df["watch_weight"] = df["watched_minutes"].apply(assign_watch_weight)
    
    for col in genre_cols:
        df[col] = df[col] * df["watch_weight"]

    # Group by user and sum weighted vectors
    user_profiles = df.groupby("user_id")[genre_cols].sum()

    return user_profiles, genre_cols

def one_hot_encode_genres(df):
    df["genres"] = df["genres"].apply(
        lambda x: x.split() if isinstance(x, str) else []
    )

    all_genres = set(g for genres in df["genres"] for g in genres)

    for genre in all_genres:
        col = f"genre_{genre.strip().replace(' ', '_').lower()}"
        df[col] = df["genres"].apply(lambda x: int(genre in x))
    return df

def train_cb_model(df):
    df = one_hot_encode_genres(df)
    genre_matrix = df[df.columns[df.columns.str.startswith("genre_")]]
    sim_matrix = cosine_similarity(genre_matrix)
    return {
        "genre_sim": sim_matrix,
        "movie_ids": df["movie_id"].tolist()
    }
