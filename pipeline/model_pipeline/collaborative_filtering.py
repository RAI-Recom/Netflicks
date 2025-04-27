from scipy.sparse import csr_matrix
import pandas as pd
from db.db_manager import DBManager
import numpy as np
import mlflow
import os
from dotenv import load_dotenv

load_dotenv()
MLFLOW_PORT = os.getenv("MLFLOW_PORT")
mlflow.set_tracking_uri("http://0.0.0.0:"+MLFLOW_PORT)
mlflow.set_experiment("Netflicks_Models")

class CustomALS:
    def __init__(self, n_components=50, regularization=0.01, learning_rate=0.1):
        self.user_factors = None
        self.item_factors = None
        self.n_components = n_components
        self.regularization = regularization
        self.learning_rate = learning_rate
        self.max_items = 0  # Track maximum number of items seen

    def _init_factors(self, batch_matrix):
        """Initialize user and item factors using SVD."""
        try:
            n_users, n_items = batch_matrix.shape
            self.max_items = max(self.max_items, n_items)  # Update max items
            self.user_factors = np.random.normal(0, 0.1, (n_users, self.n_components))
            self.item_factors = np.random.normal(0, 0.1, (self.max_items, self.n_components))
        except Exception as e:
            raise ValueError(f"Error initializing factors: {str(e)}")

    def partial_fit(self, batch_matrix, user_map, item_map):
        try:
            if self.user_factors is None:  # First batch
                self._init_factors(batch_matrix)
            else:  # Update existing factors
                self._update_factors(batch_matrix, user_map, item_map)
        except Exception as e:
            raise RuntimeError(f"Error in partial_fit: {str(e)}")

    def _update_factors(self, batch_matrix, user_map, item_map):
        try:
            n_users, n_items = batch_matrix.shape
            self.max_items = max(self.max_items, n_items)  # Update max items if needed
            
            # Ensure item_factors has enough rows
            if self.item_factors.shape[0] < self.max_items:
                new_rows = self.max_items - self.item_factors.shape[0]
                new_factors = np.random.normal(0, 0.1, (new_rows, self.n_components))
                self.item_factors = np.vstack([self.item_factors, new_factors])
            
            # Update user factors
            existing_users = [u for u in user_map if u < self.user_factors.shape[0]]
            new_users = [u for u in user_map if u >= self.user_factors.shape[0]]
            
            # Update existing users with regularization
            if existing_users:
                # Ensure we only use the relevant item factors
                relevant_items = [i for i in item_map if i < n_items]
                if relevant_items:
                    self.user_factors[existing_users] = (
                        (1 - self.learning_rate * self.regularization) * self.user_factors[existing_users] 
                        + self.learning_rate * batch_matrix[existing_users][:, relevant_items].dot(
                            self.item_factors[relevant_items]
                        )
                    )
            
            # Initialize new users
            if new_users:
                # Ensure we only use the relevant item factors
                relevant_items = [i for i in item_map if i < n_items]
                if relevant_items:
                    new_user_factors = batch_matrix[new_users][:, relevant_items].dot(
                        self.item_factors[relevant_items]
                    )
                    if self.user_factors is None:
                        self.user_factors = new_user_factors
                    else:
                        self.user_factors = np.vstack([self.user_factors, new_user_factors])
            
            # Update item factors
            existing_items = [i for i in item_map if i < self.item_factors.shape[0]]
            new_items = [i for i in item_map if i >= self.item_factors.shape[0]]
            
            # Update existing items with regularization
            if existing_items:
                # Ensure we only use the relevant user factors
                relevant_users = [u for u in user_map if u < self.user_factors.shape[0]]
                if relevant_users:
                    self.item_factors[existing_items] = (
                        (1 - self.learning_rate * self.regularization) * self.item_factors[existing_items]
                        + self.learning_rate * batch_matrix.T[existing_items][:, relevant_users].dot(
                            self.user_factors[relevant_users]
                        )
                    )
            
            # Initialize new items
            if new_items:
                # Ensure we only use the relevant user factors
                relevant_users = [u for u in user_map if u < self.user_factors.shape[0]]
                if relevant_users:
                    new_item_factors = batch_matrix.T[new_items][:, relevant_users].dot(
                        self.user_factors[relevant_users]
                    )
                    self.item_factors = np.vstack([self.item_factors, new_item_factors])
                
        except Exception as e:
            raise RuntimeError(f"Error updating factors: {str(e)}")


class CollaborativeFiltering:
    def __init__(self, n_components: int = 50, random_state: int = 42, regularization: float = 0.01, learning_rate: float = 0.1):
        """
        Initialize the CollaborativeFiltering model.

        Args:
            n_components: Number of components for SVD.
            random_state: Random state for reproducibility.
            regularization: Regularization parameter for ALS.
            learning_rate: Learning rate for factor updates.
        """
        try:
            self.n_components = n_components
            self.random_state = random_state
            self.als = CustomALS(
                n_components=n_components,
                regularization=regularization,
                learning_rate=learning_rate
            )
            self.user_rating_count = {}  # Initialize empty dict to accumulate counts
            self.db_manager = DBManager()
            np.random.seed(random_state)  # Set random seed for reproducibility
        except Exception as e:
            raise RuntimeError(f"Error initializing CollaborativeFiltering: {str(e)}")

    def train(self):
        """
        Train the collaborative filtering model using the provided DataFrame.
        """
        try:
            offset = 0
            limit = 10000
            
            while True:
                try:
                    # Load ratings chunk
                    ratings_df = self.db_manager.load_ratings_chunk(limit=limit, offset=offset)
                    if ratings_df.empty or offset > 150000:
                        break  # Exit loop if no more data
                    
                    # Clean and prepare data
                    ratings_df = ratings_df.dropna(subset=["rating"]).copy()
                    
                    # Encode user and movie IDs
                    ratings_df['user'] = ratings_df['user_id'].astype("category").cat.codes
                    ratings_df['movie'] = ratings_df['movie_id'].astype("category").cat.codes

                    # Build sparse rating matrix
                    user_item_matrix = csr_matrix((ratings_df['rating'], (ratings_df['user'], ratings_df['movie'])))

                    # Update factors
                    user_map = ratings_df['user'].values
                    item_map = ratings_df['movie'].values
                    self.als.partial_fit(user_item_matrix, user_map, item_map)

                    # Update user rating counts
                    try:
                        batch_counts = ratings_df.groupby("user_id")["rating"].count()
                        for user_id, count in batch_counts.items():
                            self.user_rating_count[user_id] = self.user_rating_count.get(user_id, 0) + count
                    except Exception as e:
                        print(f"Warning: Error updating user rating counts: {str(e)}")
                        continue  # Continue with next batch even if count update fails

                    # Update offset for the next batch
                    offset += limit
                    # print(f"Processed batch {offset}")

                except Exception as e:
                    print(f"Warning: Error processing batch at offset {offset}: {str(e)}")
                    offset += limit  # Skip to next batch
                    continue

        except Exception as e:
            raise RuntimeError(f"Error in training process: {str(e)}")

    def get_model_info(self):
        """
        Get information about the trained model.

        Returns:
            dict: A dictionary containing model information.
        """
        try:
            return {
                "user_factors": self.als.user_factors,
                "item_factors": self.als.item_factors,
                "user_rating_count": self.user_rating_count
            }
        except Exception as e:
            raise RuntimeError(f"Error getting model info: {str(e)}")
    
    def log_model_to_mlflow(self):
        try:
            with mlflow.start_run(run_name="CollaborativeFiltering_Model"):
                # Log parameters
                mlflow.log_param("n_components", self.n_components)
                mlflow.log_param("random_state", self.random_state)

                # Log metrics - example: how many users
                mlflow.log_metric("num_users", self.als.user_factors.shape[0])
                mlflow.log_metric("num_items", self.als.item_factors.shape[0])

                # # Log model artifacts
                # # Save model manually, because it's a custom class
                # model_info = self.get_model_info()

                # # Save numpy arrays temporarily
                # os.makedirs("model_artifacts", exist_ok=True)
                # np.save("model_artifacts/user_factors.npy", model_info["user_factors"])
                # np.save("model_artifacts/item_factors.npy", model_info["item_factors"])

                # Log artifacts
                # mlflow.log_artifacts("models/cf_model.pkl")
            
                print("Model logged to MLflow successfully.")

        except Exception as e:
            raise RuntimeError(f"Error logging model to MLflow: {str(e)}")
