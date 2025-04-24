from sklearn.decomposition import TruncatedSVD
from scipy.sparse import csr_matrix
import pandas as pd

class CollaborativeFiltering:
    def __init__(self, n_components: int = 50, random_state: int = 42):
        """
        Initialize the CollaborativeFiltering model.

        Args:
            n_components: Number of components for SVD.
            random_state: Random state for reproducibility.
        """
        self.n_components = n_components
        self.random_state = random_state
        self.svd = None
        self.user_factors = None
        self.item_factors = None
        self.user_item_matrix = None
        self.user_rating_count = {}

    def train(self, df: pd.DataFrame):
        """
        Train the collaborative filtering model using the provided DataFrame.

        Args:
            df: DataFrame containing user_id, movie_id, and rating columns.
        """
        # Encode user and movie IDs
        df['user'] = df['user_id'].astype("category").cat.codes
        df['movie'] = df['movie_id'].astype("category").cat.codes

        # Build sparse rating matrix
        self.user_item_matrix = csr_matrix((df['rating'], (df['user'], df['movie'])))

        # Train SVD model
        self.svd = TruncatedSVD(n_components=self.n_components, random_state=self.random_state)
        self.user_factors = self.svd.fit_transform(self.user_item_matrix)
        self.item_factors = self.svd.components_

        # Track number of ratings per user_id (original IDs)
        self.user_rating_count = df.groupby("user_id")["rating"].count().to_dict()

    def get_model_info(self):
        """
        Get information about the trained model.

        Returns:
            dict: A dictionary containing model information.
        """
        return {
            "svd": self.svd,
            "user_factors": self.user_factors,
            "item_factors": self.item_factors,
            "user_item_matrix": self.user_item_matrix,
            "user_rating_count": self.user_rating_count
        }
