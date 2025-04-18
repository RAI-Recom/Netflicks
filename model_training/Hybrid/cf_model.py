import pandas as pd
import pickle
import numpy as np
from sklearn.decomposition import TruncatedSVD
from sklearn.neighbors import NearestNeighbors

# Load dataset
ratings_df = pd.read_csv("data/hybrid_model_data.csv")
explicit_ratings = ratings_df.dropna(subset=["rating_x"])

import pandas as pd
from scipy.sparse import csr_matrix

# Assuming explicit_ratings is already defined
# Group by user_id and movie_title, and aggregate ratings
grouped = explicit_ratings.groupby(['user_id', 'movie_title'])['rating_x'].mean().reset_index()

# Create a sparse matrix
user_ids = grouped['user_id'].astype('category').cat.codes
movie_titles = grouped['movie_title'].astype('category').cat.codes

# Create a sparse matrix
user_item_matrix = csr_matrix((grouped['rating_x'], (user_ids, movie_titles)),
                               shape=(len(grouped['user_id'].unique()), len(grouped['movie_title'].unique())))


# Method 1: Matrix Factorization with TruncatedSVD
svd = TruncatedSVD(n_components=50, random_state=42)
user_factors = svd.fit_transform(user_item_matrix)
item_factors = svd.components_

# Method 2: User-User Collaborative Filtering with KNN
knn_model = NearestNeighbors(metric='cosine', algorithm='brute')
knn_model.fit(user_item_matrix)

# Save components for prediction
model_artifacts = {
    'svd': svd,
    'user_item_matrix': user_item_matrix,
    'user_factors': user_factors,
    'item_factors': item_factors,
    'knn_model': knn_model
}

with open("models/sklearn_cf_model.pkl", "wb") as f:
    pickle.dump(model_artifacts, f)

print("Collaborative Filtering model saved using scikit-learn.")



# # train_cf_model.py
# import pandas as pd
# import pickle
# from surprise import SVD, Dataset, Reader
# from surprise.model_selection import train_test_split

# # Load dataset
# ratings_df = pd.read_csv("processed_data/hybrid_model_data.csv")
# explicit_ratings = ratings_df.dropna(subset=["rating_x"])

# # Prepare Surprise dataset
# reader = Reader(rating_scale=(0, 5))
# data = Dataset.load_from_df(explicit_ratings[["user_id", "movie_title", "rating_x"]], reader)
# trainset, _ = train_test_split(data, test_size=0.2, random_state=42)

# # Train CF model
# cf_model = SVD(n_factors=50, random_state=42)
# cf_model.fit(trainset)

# # Save model
# with open("models/svd_cf_model.pkl", "wb") as f:
#     pickle.dump(cf_model, f)
# print("Collaborative Filtering model saved.")