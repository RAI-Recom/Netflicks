import sys

from pipeline.model_pipeline.popularity_model import PopularityModel
sys.path.append('.')

import logging
import pickle
import numpy as np
from typing import Dict, Any, List
import pandas as pd
from db.db_manager import DBManager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ModelValidator:
    """
    A class to validate the trained recommendation models.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the model validator.
        
        Args:
            config: Configuration dictionary containing:
                - model_paths: Dictionary of model paths
                - n_components: Number of components for CF model
        """
        self.config = config
        self.db_manager = DBManager()
        self.cf_model = None
        self.cb_model = None
        self.user_profiles = None
        self.movie_vectors = None
        self.top_popular_movies = None
        self.movie_id_to_title = None
        self._load_models()
        
    def _load_models(self) -> None:
        """Load all required models with error handling."""
        try:
            # Load collaborative filtering model
            with open(self.config["model_paths"]["cf_model"], "rb") as f:
                self.cf_model = pickle.load(f)
                if not isinstance(self.cf_model, dict):
                    raise ValueError("Invalid CF model format")
            
            # Load content-based model
            with open(self.config["model_paths"]["cb_model"], "rb") as f:
                self.cb_model = pickle.load(f)
                if not isinstance(self.cb_model, dict):
                    raise ValueError("Invalid CB model format")
            
            # Load user profiles
            self.user_profiles = pd.read_pickle(self.config["model_paths"]["user_profiles"])
            if not isinstance(self.user_profiles, pd.DataFrame):
                raise ValueError("Invalid user profiles format")
            
            # Load movie vectors
            self.movie_vectors = pd.read_pickle(self.config["model_paths"]["movie_vectors"])
            if not isinstance(self.movie_vectors, pd.DataFrame):
                raise ValueError("Invalid movie vectors format")
            
            # Load popular movies
            with open(self.config["model_paths"]["popular_movies"], "rb") as f:
                self.top_popular_movies = pickle.load(f)
                if not isinstance(self.top_popular_movies, list):
                    raise ValueError("Invalid popular movies format")
            
            # Load movie title mapping
            self.movie_id_to_title = self.db_manager.fetch_movie_title_map()
            if not isinstance(self.movie_id_to_title, dict):
                raise ValueError("Invalid movie title mapping")
                
        except FileNotFoundError as e:
            logger.error(f"Model file not found: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error loading models: {str(e)}")
            raise
            
    def _validate_user_id(self, user_id: int) -> bool:
        """Validate if user exists in the system."""
        return user_id in self.user_profiles.index or user_id in self.cf_model.get("user_rating_count", {})
        
    def _get_cb_scores(self, user_id: int) -> Dict[int, float]:
        """
        Get content-based recommendation scores using the trained CB model.
        
        Args:
            user_id: User ID to get recommendations for
            
        Returns:
            Dictionary of movie_id to similarity score
        """
        try:
            if user_id not in self.user_profiles.index:
                return {}
            
            # Get user's genre preferences
            user_genres = self.user_profiles.loc[user_id].values
            
            # Calculate similarities in batches to avoid memory issues
            batch_size = 1000
            scores = {}
            
            for i in range(0, len(self.movie_vectors), batch_size):
                batch_vectors = self.movie_vectors.iloc[i:i+batch_size]
                batch_similarities = np.dot(batch_vectors.values, user_genres)
                
                # Store scores for this batch
                for movie_id, score in zip(batch_vectors.index, batch_similarities):
                    scores[movie_id] = score
            
            return scores
            
        except Exception as e:
            logger.error(f"Error calculating CB scores: {str(e)}")
            return {}
            
    def _get_cf_scores(self, user_id: int, movie_ids: List[int]) -> Dict[int, float]:
        """Get collaborative filtering recommendation scores."""
        try:
            scores = {}
            for movie_id in movie_ids:
                try:
                    pred = self.cf_model.predict(str(user_id), str(movie_id)).est
                    scores[movie_id] = pred
                    # if(scores[movie_id] > 0):
                        # logger.info(f"User {user_id} rated movie {movie_id} with score {scores[movie_id]}")
                except Exception:
                    continue
            return scores
            
        except Exception as e:
            logger.error(f"Error calculating CF scores: {str(e)}")
            return {}
            
    def validate_popularity_model(self) -> Dict[str, Any]:
        """Validate the popularity model."""
        try:
            logger.info("Validating popularity model...")
            
            # Get recommendations from popular movies
            recommendations = [self.movie_id_to_title.get(mid, f"Unknown Title ({mid})") 
                            for mid in self.top_popular_movies[:20]]
            
            metrics = {
                "model_type": "popularity",
                "recommendations_generated": True,
                "num_recommendations": len(recommendations),
                "recommendations": recommendations
            }
            
            logger.info(f"Popularity model validation complete: {metrics}")
            return metrics
            
        except Exception as e:
            logger.error(f"Error validating popularity model: {str(e)}")
            raise
            
    def validate_content_based_model(self) -> Dict[str, Any]:
        """Validate the content-based filtering model."""
        try:
            logger.info("Validating content-based filtering model...")
            
            # Get a random user from the database
            ratings_df = self.db_manager.load_ratings_chunk(limit=1000)
            if ratings_df.empty:
                raise ValueError("No users found in database")
            
            test_user_id = 263960
            
            # Get CB scores
            cb_scores = self._get_cb_scores(test_user_id)
            
            if cb_scores:
                # Get top 5 recommendations
                ranked = sorted(cb_scores.items(), key=lambda x: x[1], reverse=True)
                recommendations = [self.movie_id_to_title.get(movie_id, f"Unknown Title ({movie_id})") 
                                for movie_id, _ in ranked[:20]]
                
                metrics = {
                    "model_type": "content_based",
                    "recommendations_generated": True,
                    "num_recommendations": len(recommendations),
                    "test_user_id": test_user_id,
                    "recommendations": recommendations
                }
            else:
                metrics = {
                    "model_type": "content_based",
                    "recommendations_generated": False,
                    "error": "No recommendations generated"
                }
            
            logger.info(f"Content-based model validation complete: {metrics}")
            return metrics
            
        except Exception as e:
            logger.error(f"Error validating content-based model: {str(e)}")
            raise
            
    def validate_collaborative_filtering_model(self) -> Dict[str, Any]:
        """Validate the collaborative filtering model."""
        try:
            logger.info("Validating collaborative filtering model...")
            
            # Get a random user from the database
            ratings_df = self.db_manager.load_ratings_chunk(limit=1000)
            # logger.info(f"Ratings DataFrame: {ratings_df}")
            if ratings_df.empty:
                raise ValueError("No users found in database")
            
            test_user_id = 265143
            
            # Get all movie IDs
            all_movie_ids = self.movie_vectors.index.tolist()
            
            # Get CF scores
            cf_scores = self._get_cf_scores(test_user_id, all_movie_ids)
            
            if cf_scores:
                # Get top 5 recommendations
                ranked = sorted(cf_scores.items(), key=lambda x: x[1], reverse=True)
                recommendations = [self.movie_id_to_title.get(movie_id, f"Unknown Title ({movie_id})") 
                                for movie_id, _ in ranked[:5]]
                
                metrics = {
                    "model_type": "collaborative_filtering",
                    "recommendations_generated": True,
                    "num_recommendations": len(recommendations),
                    "test_user_id": test_user_id,
                    "recommendations": recommendations
                }
            else:
                metrics = {
                    "model_type": "collaborative_filtering",
                    "recommendations_generated": False,
                    "error": "No recommendations generated"
                }
            
            logger.info(f"Collaborative filtering model validation complete: {metrics}")
            return metrics
            
        except Exception as e:
            logger.error(f"Error validating collaborative filtering model: {str(e)}")
            raise
            
    def run_validation(self) -> Dict[str, Any]:
        """
        Run validation for all models.
        
        Returns:
            Dictionary containing validation results for all models
        """
        try:
            logger.info("Starting model validation...")
            
            results = {
                "popularity_model": self.validate_popularity_model(),
                "content_based_model": self.validate_content_based_model(),
                # "collaborative_filtering_model": self.validate_collaborative_filtering_model()
            }
            
            # Check if all validations passed
            all_valid = all(
                result.get("recommendations_generated", False) 
                for result in results.values()
            )
            
            results["all_valid"] = all_valid
            
            # Log detailed results
            for model_name, metrics in results.items():
                if model_name != "all_valid":
                    logger.info(f"\n{model_name} validation results:")
                    for key, value in metrics.items():
                        if key != "recommendations":  # Don't log full recommendations
                            logger.info(f"  {key}: {value}")
            
            if not all_valid:
                logger.error("Model validation failed")
                sys.exit(1)
            
            logger.info("✅ Model validation completed successfully")
            return results
            
        except Exception as e:
            logger.error(f"Error during model validation: {str(e)}")
            raise

if __name__ == "__main__":
    # Configuration
    config = {
        "model_paths": {
            "cf_model": "models/cf_model.pkl",
            "cb_model": "models/cb_model.pkl",
            "user_profiles": "models/user_profiles.pkl",
            "movie_vectors": "models/movie_vectors.pkl",
            "popular_movies": PopularityModel.Popularity_artifact_uri
        },
        "n_components": 50
    }
    
    # Run validation
    validator = ModelValidator(config)
    results = validator.run_validation()
    
    # Print results
    print("\nValidation Results:")
    for model_name, metrics in results.items():
        if model_name != "all_valid":
            print(f"\n{model_name}:")
            for key, value in metrics.items():
                if key != "recommendations":  # Don't print full recommendations
                    print(f"  {key}: {value}")
    
    # Exit with appropriate status code
    sys.exit(0 if results["all_valid"] else 1) 