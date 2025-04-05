# train_cf_model.py
import pandas as pd
import pickle
from surprise import SVD, Dataset, Reader
from surprise.model_selection import train_test_split

# Load dataset
ratings_df = pd.read_csv("processed_data/hybrid_model_data.csv")
explicit_ratings = ratings_df.dropna(subset=["rating_x"])

# Prepare Surprise dataset
reader = Reader(rating_scale=(0, 5))
data = Dataset.load_from_df(explicit_ratings[["user_id", "movie_title", "rating_x"]], reader)
trainset, _ = train_test_split(data, test_size=0.2, random_state=42)

# Train CF model
cf_model = SVD(n_factors=50, random_state=42)
cf_model.fit(trainset)

# Save model
with open("models/svd_cf_model.pkl", "wb") as f:
    pickle.dump(cf_model, f)
print("Collaborative Filtering model saved.")