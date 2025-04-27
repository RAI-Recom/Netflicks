import pandas as pd
import pickle
import logging
from typing import Dict, List, Tuple, Optional
from sklearn.metrics.pairwise import cosine_similarity
from db.db_manager import DBManager

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HybridRecommender:
    def __init__(self, model_paths: Dict[str, str]):
        """
        Initialize the hybrid recommender system.
        
        Args:
            model_paths: Dictionary containing paths to model files
        """
        self.model_paths = model_paths
        self.cf_model = None
        self.cb_model = None
        self.user_profiles = None
        self.movie_vectors = None
        self.top_popular_movies = None
        self.db_manager = DBManager()
        self.movie_id_to_title = None
        self._load_models()
        
    def _load_models(self) -> None:
        """Load all required models with error handling."""
        try:
            # Load collaborative filtering model
            with open(self.model_paths["cf_model"], "rb") as f:
                self.cf_model = pickle.load(f)
                if not isinstance(self.cf_model, dict):
                    raise ValueError("Invalid CF model format")
            
            # Load content-based model
            with open(self.model_paths["cb_model"], "rb") as f:
                self.cb_model = pickle.load(f)
                if not isinstance(self.cb_model, dict):
                    raise ValueError("Invalid CB model format")
            
            # Load user profiles
            self.user_profiles = pd.read_pickle(self.model_paths["user_profiles"])
            if not isinstance(self.user_profiles, pd.DataFrame):
                raise ValueError("Invalid user profiles format")
            
            # Load movie vectors
            self.movie_vectors = pd.read_pickle(self.model_paths["movie_vectors"])
            if not isinstance(self.movie_vectors, pd.DataFrame):
                raise ValueError("Invalid movie vectors format")
            
            # Load popular movies
            with open(self.model_paths["popular_movies"], "rb") as f:
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
        
    def get_weights(self, user_id: int) -> Tuple[float, float]:
        """
        Calculate weights for hybrid recommendation based on user activity.
        
        Args:
            user_id: User ID to calculate weights for
            
        Returns:
            Tuple of (CF weight, CB weight)
        """
        try:
            if not self._validate_user_id(user_id):
                logger.warning(f"User {user_id} not found in system")
                return 0.0, 1.0  # Default to content-based
            
            # Get rating count
            n_rated = self.cf_model["user_rating_count"].get(user_id, 0)
            
            # Calculate weights based on rating count
            if n_rated >= 20:  # Heavy user
                return 0.8, 0.2
            elif n_rated >= 10:  # Medium user
                return 0.6, 0.4
            elif n_rated > 0:  # Light user
                return 0.4, 0.6
            else:  # New user
                return 0.0, 1.0
                
        except Exception as e:
            logger.error(f"Error calculating weights: {str(e)}")
            return 0.0, 1.0  # Fallback to content-based
            
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
            user_genres = self.user_profiles.loc[user_id]
            
            # Get all movie vectors
            movie_vectors = self.movie_vectors
            
            # Calculate similarity scores for all movies
            scores = {}
            for movie_id in movie_vectors.index:
                # Calculate cosine similarity between user preferences and movie vector
                similarity = cosine_similarity(
                    user_genres.values.reshape(1, -1),
                    movie_vectors.loc[movie_id].values.reshape(1, -1)
                )[0][0]
                scores[movie_id] = similarity
            
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
                except Exception:
                    continue
            return scores
            
        except Exception as e:
            logger.error(f"Error calculating CF scores: {str(e)}")
            return {}
            
    def recommend(self, user_id: int, top_n: int = 20) -> List[str]:
        """
        Generate hybrid recommendations for a user.
        
        Args:
            user_id: User ID to generate recommendations for
            top_n: Number of recommendations to return
            
        Returns:
            List of recommended movie titles
        """
        try:
            # Validate input
            if not isinstance(user_id, int) or not isinstance(top_n, int):
                raise ValueError("Invalid input types")
                
            if top_n <= 0:
                raise ValueError("top_n must be positive")
                
            # Get weights
            alpha, beta = self.get_weights(user_id)
            
            # Get all movie IDs
            all_movie_ids = self.movie_vectors.index.tolist()
            
            # Get scores from both models
            cb_scores = self._get_cb_scores(user_id)
            cf_scores = self._get_cf_scores(user_id, all_movie_ids)
            
            # Combine scores
            final_scores = {}
            for movie_id in all_movie_ids:
                cf_score = cf_scores.get(movie_id, 0)
                cb_score = cb_scores.get(movie_id, 0)
                score = alpha * cf_score + beta * cb_score
                if score > 0:
                    final_scores[movie_id] = score
                    
            # Return recommendations
            if final_scores:
                ranked = sorted(final_scores.items(), key=lambda x: x[1], reverse=True)
                logger.info(f"Generated {len(ranked)} hybrid recommendations for user {user_id}")
                return [self.movie_id_to_title.get(movie_id, f"Unknown Title ({movie_id})") 
                        for movie_id, _ in ranked[:top_n]]
            else:
                logger.info(f"Using popular movies for user {user_id}")
                return [self.movie_id_to_title.get(mid, f"Unknown Title ({mid})") 
                        for mid in self.top_popular_movies[:top_n]]
                        
        except Exception as e:
            logger.error(f"Error generating recommendations: {str(e)}")
            # Fallback to popular movies
            return [self.movie_id_to_title.get(mid, f"Unknown Title ({mid})") 
                    for mid in self.top_popular_movies[:top_n]]

# Initialize recommender
recommender = HybridRecommender({
    "cf_model": "models/cf_model.pkl",
    "cb_model": "models/cb_model.pkl",
    "user_profiles": "models/user_profiles.pkl",
    "movie_vectors": "models/movie_vectors.pkl",
    "popular_movies": "models/popular_movies.pkl"
})

# Wrapper function for backward compatibility
def hybrid_recommend(user_id: int, top_n: int = 20) -> List[str]:
    """Wrapper function for the hybrid recommender."""
    return recommender.recommend(user_id, top_n)
