from pipeline import load_data, preprocess, train_cf, train_cb, save_model, profile_builder, generate_pop
import pandas as pd
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
generate_pop.train_and_save_popularity(movie_df,path="evaluation/pop_model.pkl", top_n=20)

# --- Preprocess for CF/CB ---
cf_df = preprocess.preprocess_cf(train_ratings_df)
cb_df = preprocess.preprocess_cb(watch_df)

# --- Build User & Movie Profiles ---
user_profiles, genre_cols = train_cb.build_user_genre_profiles(watch_df)
movie_vectors = profile_builder.build_movie_genre_vectors(watch_df, genre_cols)
profile_builder.save_profiles(user_profiles, movie_vectors, user_path = "evaluation/user_profiles.pkl", movie_path="evaluation/movie_vectors.pkl")

# --- Train Models ---
cf_model = train_cf.train_cf_model(cf_df)
cb_model = train_cb.train_cb_model(cb_df)

# --- Save to separate folder for evaluation ---
save_model.save_model(cf_model, f"evaluation/cf_model.pkl")
save_model.save_model(cb_model, f"evaluation/cb_model.pkl")
