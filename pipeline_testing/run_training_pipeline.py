from pipeline import load_data, preprocess, train_cf, train_cb, save_model, generate_pop
from pipeline import profile_builder  

# Load data
ratings_df = load_data.load_ratings_chunk(limit=5000, offset=0)
watch_df = load_data.load_watch_chunk(limit=5000, offset=0)
movie_df = load_data.load_movies()

# Train popularity model
# generate_pop.train_and_save_popularity(movie_df, top_n=20)

# Preprocess data for CF and CB models
cf_df = preprocess.preprocess_cf(ratings_df)
cb_df = preprocess.preprocess_cb(watch_df)

# Build and save profiles
user_profiles, genre_cols = train_cb.build_user_genre_profiles(watch_df)
movie_vectors = profile_builder.build_movie_genre_vectors(watch_df, genre_cols)

# # Train and save models
cf_model = train_cf.train_cf_model(cf_df)
cb_model = train_cb.train_cb_model(cb_df)
# save_model.save_model(cf_model, "models/cf_model.pkl")
# save_model.save_model(cb_model, "models/cb_model.pkl")
