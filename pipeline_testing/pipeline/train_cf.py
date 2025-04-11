from sklearn.decomposition import TruncatedSVD
from scipy.sparse import csr_matrix

def train_cf_model(df):
    # Encode user and movie IDs
    df['user'] = df['user_id'].astype("category").cat.codes
    df['movie'] = df['movie_id'].astype("category").cat.codes

    # Build sparse rating matrix
    user_item_matrix = csr_matrix((df['rating'], (df['user'], df['movie'])))

    # Train SVD model
    svd = TruncatedSVD(n_components=50, random_state=42)
    user_factors = svd.fit_transform(user_item_matrix)
    item_factors = svd.components_

    # Track number of ratings per user_id (original IDs)
    user_rating_count = df.groupby("user_id")["rating"].count().to_dict()

    # Build model dictionary
    return {
        "svd": svd,
        "user_factors": user_factors,
        "item_factors": item_factors,
        "user_item_matrix": user_item_matrix,
        "user_rating_count": user_rating_count
    }
