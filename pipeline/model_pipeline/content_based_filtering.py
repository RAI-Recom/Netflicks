from sklearn.metrics.pairwise import cosine_similarity
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any, Optional
import re
from db.db_manager import DBManager
import logging
import mlflow
import os
from dotenv import load_dotenv
import json

load_dotenv()
MLFLOW_PORT = os.getenv("MLFLOW_PORT")
mlflow.set_tracking_uri("http://0.0.0.0:"+MLFLOW_PORT)
mlflow.set_experiment("Netflicks_Models")

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ContentBasedFiltering:
    """
    A class for content-based filtering recommendation system.
    """
    
    def __init__(self, batch_size: int = 10000):
        """Initialize the ContentBasedFiltering model."""
        logger.info("Initializing ContentBasedFiltering model")
        self.genre_cols = []
        self.genre_matrix = None
        self.sim_matrix = None
        self.movie_ids = []
        self.model = {}
        self.batch_size = batch_size
        self.genre_mapping = {}  # Map genres to standardized names
        self.db_manager = DBManager()
        self.user_profiles = None
        self.movie_vectors = None
        # logger.info(f"ContentBasedFiltering initialized with batch_size={batch_size}")
        
    def _standardize_genre(self, genre: str) -> str:
        """Standardize genre names by removing special characters and converting to lowercase."""
        return genre.strip().lower().replace(' ', '_').replace('-', '_')
        
    def assign_watch_weight(self, minutes: float) -> float:
        """
        Assign a weight to a watch session based on minutes watched using a sigmoid function.
        
        Args:
            minutes: Number of minutes watched
            
        Returns:
            Weight value between 0 and 1
        """
        if pd.isna(minutes): 
            return 0.0
            
        # Sigmoid function parameters
        k = 0.05  # Steepness
        x0 = 40   # Midpoint
        
        # Sigmoid function: f(x) = 1 / (1 + e^(-k(x-x0)))
        weight = 1 / (1 + np.exp(-k * (minutes - x0)))
        return weight
    
    def process_genres(self, genres_str: str) -> List[str]:
        """
        Process and standardize genre strings.
        
        Args:
            genres_str: String containing genres
            
        Returns:
            List of standardized genre names
        """
        # Handle None or empty values
        if genres_str is None or (isinstance(genres_str, (list, np.ndarray)) and len(genres_str) == 0):
            return []
            
        # If genres_str is already a list, process each element
        if isinstance(genres_str, (list, np.ndarray)):
            genres = []
            for genre in genres_str:
                if pd.isna(genre):
                    continue
                genre = self._standardize_genre(str(genre))
                if genre and genre not in genres:
                    genres.append(genre)
                    if genre not in self.genre_mapping:
                        self.genre_mapping[genre] = genre
            return genres
            
        # Handle string input
        if pd.isna(genres_str):
            return []
            
        # Split by common delimiters and clean
        genres = []
        for genre in re.split(r'[,\s]+', str(genres_str)):
            genre = self._standardize_genre(genre)
            if genre and genre not in genres:
                genres.append(genre)
                if genre not in self.genre_mapping:
                    self.genre_mapping[genre] = genre
        return genres
    
    def build_user_genre_profiles(self, user_profiles_path: str = "models/user_profiles.pkl"
                                ) -> Tuple[pd.DataFrame, List[str]]:
        """
        Build user genre profiles from watch history with improved weighting.
        
        Args:
            user_profiles_path: Path to save user profiles
            
        Returns:
            Tuple of (user profiles DataFrame, list of genre column names)
        """
        try:
            # logger.info("Starting to build user genre profiles")
            offset = 0
            all_watch_data = []
            
            # Load watch history in batches
            while True:
                # logger.info(f"Loading watch history batch at offset {offset}")
                watch_df = self.db_manager.load_watch_chunk(
                    limit=self.batch_size,
                    offset=offset
                )
                
                if watch_df.empty or offset > 10000:
                    # logger.info("Finished loading watch history data")
                    break
                    
                # Process genres
                # logger.info("Processing genres for current batch")
                watch_df["processed_genres"] = watch_df["genres"].apply(self.process_genres)
                all_watch_data.append(watch_df)
                
                offset += self.batch_size
                # logger.info(f"Processed {offset} watch history records")
            
            # Combine all batches
            # logger.info("Combining all watch history batches")
            watch_df = pd.concat(all_watch_data, ignore_index=True)
            
            # Get all unique genres
            # logger.info("Extracting unique genres")
            all_genres = set()
            for genres in watch_df["processed_genres"]:
                all_genres.update(genres)
            
            # Create genre columns
            # logger.info("Creating genre columns")
            genre_cols = []
            for genre in sorted(all_genres):
                col = f"genre_{genre}"
                watch_df[col] = watch_df["processed_genres"].apply(lambda x: int(genre in x))
                genre_cols.append(col)
            
            # Apply watch weight using sigmoid function
            # logger.info("Applying watch weights")
            watch_df["watch_weight"] = watch_df["watched_minutes"].apply(self.assign_watch_weight)
            
            # Calculate watch frequency for each user-movie pair
            # logger.info("Calculating watch frequencies")
            watch_df["watch_frequency"] = watch_df.groupby(["user_id", "movie_id"])["movie_id"].transform("count")
            
            # Weight genre columns by watch weight and frequency
            # logger.info("Weighting genre columns")
            for col in genre_cols:
                watch_df[col] = (
                    watch_df[col] * 
                    watch_df["watch_weight"] * 
                    watch_df["watch_frequency"]
                )
            
            # Group by user and sum weighted vectors
            # logger.info("Creating user profiles")
            self.user_profiles = watch_df.groupby("user_id")[genre_cols].sum()
            
            # Normalize user profiles
            # logger.info("Normalizing user profiles")
            row_sums = self.user_profiles.sum(axis=1)
            self.user_profiles = self.user_profiles.div(row_sums, axis=0).fillna(0)
            
            # Save profiles
            # logger.info(f"Saving user profiles to {user_profiles_path}")
            self.user_profiles.to_pickle(user_profiles_path)
            self.genre_cols = genre_cols
            
            logger.info("User genre profiles built successfully")
            return self.user_profiles, self.genre_cols
            
        except Exception as e:
            logger.error(f"Error building user genre profiles: {str(e)}")
            raise RuntimeError(f"Error building user genre profiles: {str(e)}")
    
    def build_movie_genre_vectors(self, movie_vectors_path: str = "models/movie_vectors.pkl"
                                ) -> pd.DataFrame:
        """
        Creates a movie feature matrix using genre columns with improved processing.
        
        Args:
            movie_vectors_path: Path to save movie vectors
            
        Returns:
            DataFrame representing movie feature vectors
        """
        try:
            # logger.info("Starting to build movie genre vectors")
            # Load movies in batches
            offset = 0
            all_movie_data = []
            
            while True:
                # logger.info(f"Loading movie batch at offset {offset}")
                movies_df = self.db_manager.load_movies(limit=self.batch_size, offset=offset)
                if movies_df.empty or offset > 100000:
                    # logger.info("Finished loading movie data")
                    break
                    
                # Process genres
                # logger.info("Processing genres for current batch")
                movies_df["processed_genres"] = movies_df["genres"].apply(self.process_genres)
                all_movie_data.append(movies_df)
                
                offset += self.batch_size
                # logger.info(f"Processed {offset} movie records")
            
            # Combine all batches
            # logger.info("Combining all movie batches")
            movies_df = pd.concat(all_movie_data, ignore_index=True)
            
            # Create genre columns
            # logger.info("Creating genre columns for movies")
            for col in self.genre_cols:
                genre = col.replace("genre_", "")
                movies_df[col] = movies_df["processed_genres"].apply(lambda x: int(genre in x))
            
            # Create movie vectors
            # logger.info("Creating movie vectors")
            self.movie_vectors = movies_df.set_index("movie_id")[self.genre_cols]
            
            # Normalize movie vectors
            # logger.info("Normalizing movie vectors")
            row_sums = self.movie_vectors.sum(axis=1)
            self.movie_vectors = self.movie_vectors.div(row_sums, axis=0).fillna(0)
            
            # Save vectors
            # logger.info(f"Saving movie vectors to {movie_vectors_path}")
            self.movie_vectors.to_pickle(movie_vectors_path)
            
            logger.info("Movie genre vectors built successfully")
            return self.movie_vectors
            
        except Exception as e:
            logger.error(f"Error building movie genre vectors: {str(e)}")
            raise RuntimeError(f"Error building movie genre vectors: {str(e)}")
    
    def train(self) -> Dict[str, Any]:
        """
        Train the content-based filtering model with improved processing.
        
        Returns:
            Dictionary containing the trained model
        """
        try:
            logger.info("Starting content-based filtering model training")
            
            # Build user and movie profiles
            logger.info("Building user genre profiles")
            self.build_user_genre_profiles()
            
            logger.info("Building movie genre vectors")
            self.build_movie_genre_vectors()
            
            # Instead of computing full similarity matrix, store normalized vectors
            # and compute similarities on-demand
            logger.info("Storing normalized movie vectors")
            self.movie_vectors = self.movie_vectors.astype(np.float32)  # Reduce memory usage
            
            # Store model components
            logger.info("Storing model components")
            self.model = {
                "movie_vectors": self.movie_vectors,  # Store normalized vectors instead of similarity matrix
                "movie_ids": self.movie_vectors.index.tolist(),
                "genre_mapping": self.genre_mapping
            }
            
            logger.info("Content-based filtering model training completed successfully")
            return self.model
            
        except Exception as e:
            logger.error(f"Error training content-based model: {str(e)}")
            raise RuntimeError(f"Error training content-based model: {str(e)}")
    
    def get_recommendations(self, movie_id: int, n_recommendations: int = 5) -> List[int]:
        """
        Get movie recommendations based on content similarity with improved error handling.
        
        Args:
            movie_id: ID of the movie to base recommendations on
            n_recommendations: Number of recommendations to return
            
        Returns:
            List of recommended movie IDs
        """
        try:
            logger.info(f"Getting recommendations for movie {movie_id}")
            
            if self.movie_vectors is None or self.movie_ids is None:
                logger.error("Model not trained. Call train() first.")
                raise ValueError("Model not trained. Call train() first.")
            
            # Find index of the movie
            try:
                movie_idx = self.movie_ids.index(movie_id)
            except ValueError:
                logger.error(f"Movie ID {movie_id} not found in training data")
                raise ValueError(f"Movie ID {movie_id} not found in training data")
            
            # Get the target movie vector
            target_vector = self.movie_vectors.iloc[movie_idx].values
            
            # Calculate similarities in batches to avoid memory issues
            batch_size = 1000
            similarities = []
            
            for i in range(0, len(self.movie_vectors), batch_size):
                batch_vectors = self.movie_vectors.iloc[i:i+batch_size].values
                batch_similarities = np.dot(batch_vectors, target_vector)
                similarities.extend(batch_similarities)
            
            # Convert to numpy array for efficient sorting
            similarities = np.array(similarities)
            
            # Get indices of top N similar movies (excluding self)
            logger.info("Finding top similar movies")
            similar_indices = np.argsort(similarities)[::-1][1:n_recommendations+1]
            
            # Convert indices to movie IDs
            recommended_ids = [self.movie_ids[idx] for idx in similar_indices]
            
            logger.info(f"Successfully generated {n_recommendations} recommendations")
            return recommended_ids
            
        except Exception as e:
            logger.error(f"Error getting recommendations: {str(e)}")
            raise RuntimeError(f"Error getting recommendations: {str(e)}")
        
    def log_model_to_mlflow(self):
        try:
            with mlflow.start_run(run_name="ContentBasedFiltering_Model"):
                # Log parameters (e.g., batch_size for loading data)
                mlflow.log_param("batch_size", self.batch_size)

                # Log the number of movies and users (metrics)
                mlflow.log_metric("num_movies", len(self.movie_vectors))
                mlflow.log_metric("num_users", len(self.user_profiles))

                # Log model artifacts (the trained model components)
                model_artifact_path = "models/cb_model.pkl"
                
                mlflow.log_artifact(model_artifact_path, artifact_path="model")
                artifact_uri = mlflow.get_artifact_uri("model/cb_model.pkl")

                artifact_path = artifact_uri.replace("file://", "")

                fixed_config_dir = "/home/Recomm-project/Netflicks/artifacts2/path"
                os.makedirs(fixed_config_dir, exist_ok=True)

                config_path = os.path.join(fixed_config_dir, "cb_artifact_config.json")

                config_data = {"artifact_uri": artifact_uri}

                with open(config_path, "w") as f:
                    json.dump(config_data, f, indent=4)

                print(f"cb artifact config at: {config_path}")

                print("Model logged to MLflow successfully.")

        except Exception as e:
            raise RuntimeError(f"Error logging model to MLflow: {str(e)}")
