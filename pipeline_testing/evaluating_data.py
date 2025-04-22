from pipeline_testing.model_pipelines import load_data, preprocess, train_cf, save_model, profile_builder
import pandas as pd
from pipeline_testing.model_pipelines.popularity_model import PopularityModel
from pipeline_testing.model_pipelines import content_based_filtering

def fetch_all_chunks(load_func, chunk_size=10000):
    offset = 0
    chunks = []

    while True:
        chunk = load_func(limit=chunk_size, offset=offset)
        if chunk.empty:
            break
        chunks.append(chunk)
        offset += chunk_size

    return pd.concat(chunks, ignore_index=True)

# --- Params ---
cutoff = "2025-03-01 00:00:00"
# model_dir = "offline_models/"  # Save CF/CB models here

# --- Load Data (only TRAIN split) ---
train_ratings_df = load_data.load_eval_ratings(cutoff=cutoff, mode="train")
watch_df = load_data.load_eval_watch(cutoff=cutoff,mode="train")
movie_df = load_data.load_movies()

# --- Train Popularity Model (optional for baseline comparison) ---
popularity_model = PopularityModel()
popularity_model.train_and_save(movie_df)

# --- Preprocess for CF/CB ---
cf_df = preprocess.preprocess_collaborative_filtering_model(train_ratings_df)
cb_df = preprocess.preprocess_content_based_model(watch_df)

# --- Build User & Movie Profiles ---
user_profiles, genre_cols = content_based_filtering.build_user_genre_profiles(watch_df)
movie_vectors = profile_builder.build_movie_genre_vectors(watch_df, genre_cols)
profile_builder.save_profiles(user_profiles, movie_vectors, user_path = "evaluation/user_profiles.pkl", movie_path="evaluation/movie_vectors.pkl")

# --- Train Models ---
cf_model = train_cf.train_cf_model(cf_df)
cb_model = content_based_filtering.train_cb_model(cb_df)

# --- Save to separate folder for evaluation ---
save_model.save_model(cf_model, f"evaluation/cf_model.pkl")
save_model.save_model(cb_model, f"evaluation/cb_model.pkl")
