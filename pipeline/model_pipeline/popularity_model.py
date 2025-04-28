import pandas as pd
import pickle
import os
from typing import List, Optional
import mlflow
from dotenv import load_dotenv
import json

load_dotenv()
MLFLOW_PORT = os.getenv("MLFLOW_PORT")
mlflow.set_tracking_uri("http://0.0.0.0:"+MLFLOW_PORT)
mlflow.set_experiment("Netflicks_Models")
class PopularityModel:
    """
    A class for computing and managing movie popularity models based on Bayesian average ranking.
    """
    Popularity_artifact_uri = None
    def __init__(self, top_n: int = 50, model_path: str = "models/popular_movies.pkl"):
        """
        Initialize the PopularityModel.
        
        Args:
            top_n: Number of top popular movies to return
            model_path: Path to save/load the model
        """
        self.top_n = top_n
        self.model_path = model_path
        self.popular_movie_ids = None
        
    def compute_popularity_model(self, movie_df: pd.DataFrame) -> List[int]:
        """
        Compute Bayesian average ranking based on rating + votes.
        Returns top N popular movie_ids.
        
        Args:
            movie_df: DataFrame containing movie data with 'rating' and 'votes' columns
            
        Returns:
            List of top N popular movie IDs
        """
        # Drop rows with missing ratings or votes
        movie_df = movie_df.dropna(subset=["rating", "votes"])
        
        # Calculate the mean rating across all movies
        C = movie_df["rating"].mean()
        
        # Calculate the 90th percentile of votes to use as a minimum threshold
        m = movie_df["votes"].quantile(0.90)
        
        # Define the Bayesian score function
        def bayesian_score(row):
            v = row["votes"]
            R = row["rating"]
            return (v / (v + m)) * R + (m / (v + m)) * C
        
        # Apply the Bayesian score to each movie
        movie_df["popularity_score"] = movie_df.apply(bayesian_score, axis=1)
        
        # Sort by popularity score and get the top N movie IDs
        top_movies = movie_df.sort_values("popularity_score", ascending=False)
        self.popular_movie_ids = top_movies["movie_id"].head(self.top_n).tolist()
        
        return self.popular_movie_ids
    
    def save_model(self) -> None:
        """
        Save the popularity model to a file.
        """
        if self.popular_movie_ids is None:
            raise ValueError("No model to save. Run compute_popularity_model first.")
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(self.model_path), exist_ok=True)
        
        # Save the model
        with open(self.model_path, "wb") as f:
            pickle.dump(self.popular_movie_ids, f)
        
        # print(f"✅ Popularity model saved with top {self.top_n} movies.")
    
    def load_model(self) -> List[int]:
        """
        Load the popularity model from a file.
        
        Returns:
            List of popular movie IDs
        """
        if not os.path.exists(self.model_path):
            raise FileNotFoundError(f"Model file not found at {self.model_path}")
        
        with open(self.model_path, "rb") as f:
            self.popular_movie_ids = pickle.load(f)
        
        return self.popular_movie_ids
    
    def train_and_save(self, movie_df: pd.DataFrame) -> List[int]:
        """
        Train the popularity model and save it to a file.
        
        Args:
            movie_df: DataFrame containing movie data with 'rating' and 'votes' columns
            
        Returns:
            List of top N popular movie IDs
        """
        with mlflow.start_run(run_name="Popularity_Model_Training"):
            self.compute_popularity_model(movie_df)
            self.save_model()

            mlflow.log_param("top_n", self.top_n)

            # Log metrics
            mlflow.log_metric("num_popular_movies", len(self.popular_movie_ids))

            # Save the model file as an artifact
            mlflow.log_artifact(self.model_path, artifact_path="model")
            artifact_uri = mlflow.get_artifact_uri("model/popular_movies.pkl")

            # Remove "file://" to get the actual file system path
            artifact_path = artifact_uri.replace("file://", "")

            # Set your fixed path
            fixed_config_dir = "/home/Recomm-project/Netflicks/artifacts2/path"
            os.makedirs(fixed_config_dir, exist_ok=True)

            # Set the config file name and path
            config_path = os.path.join(fixed_config_dir, "popularity_artifact_config.json")

            # Save the artifact URI into the config
            config_data = {"artifact_uri": artifact_uri}

            with open(config_path, "w") as f:
                json.dump(config_data, f, indent=4)

            print(f"Saved config at: {config_path}")

        return self.popular_movie_ids
    
    def get_popular_movies(self, movie_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Get the popular movies with their details.
        
        Args:
            movie_df: Optional DataFrame containing movie data. If not provided, 
                     the model must be loaded first.
            
        Returns:
            DataFrame containing the popular movies with their details
        """
        if movie_df is None and self.popular_movie_ids is None:
            self.load_model()
        
        if movie_df is not None:
            return movie_df[movie_df["movie_id"].isin(self.popular_movie_ids)]
        else:
            raise ValueError("No movie data provided and no model loaded.")