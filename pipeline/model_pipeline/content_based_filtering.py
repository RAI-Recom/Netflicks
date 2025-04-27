from sklearn.metrics.pairwise import cosine_similarity
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any, Optional
import re
from db.db_manager import DBManager


class ContentBasedFiltering:
    """
    A class for content-based filtering recommendation system.
    """
    
    def __init__(self, batch_size: int = 10000):
        """Initialize the ContentBasedFiltering model."""
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
            offset = 0
            all_watch_data = []
            
            # Load watch history in batches
            while True:
                watch_df = self.db_manager.load_watch_chunk(
                    limit=self.batch_size,
                    offset=offset
                )
                
                if watch_df.empty or offset > 10000:
                    break
                    
                # Process genres
                watch_df["processed_genres"] = watch_df["genres"].apply(self.process_genres)
                all_watch_data.append(watch_df)
                
                offset += self.batch_size
                print(f"Processed {offset} watch history records")
            
            # Combine all batches
            watch_df = pd.concat(all_watch_data, ignore_index=True)
            
            # Get all unique genres
            all_genres = set()
            for genres in watch_df["processed_genres"]:
                all_genres.update(genres)
            
            # Create genre columns
            genre_cols = []
            for genre in sorted(all_genres):
                col = f"genre_{genre}"
                watch_df[col] = watch_df["processed_genres"].apply(lambda x: int(genre in x))
                genre_cols.append(col)
            
            # Apply watch weight using sigmoid function
            watch_df["watch_weight"] = watch_df["watched_minutes"].apply(self.assign_watch_weight)
            
            # Calculate watch frequency for each user-movie pair
            watch_df["watch_frequency"] = watch_df.groupby(["user_id", "movie_id"])["movie_id"].transform("count")
            
            # Weight genre columns by watch weight and frequency
            for col in genre_cols:
                watch_df[col] = (
                    watch_df[col] * 
                    watch_df["watch_weight"] * 
                    watch_df["watch_frequency"]
                )
            
            # Group by user and sum weighted vectors
            self.user_profiles = watch_df.groupby("user_id")[genre_cols].sum()
            
            # Normalize user profiles
            row_sums = self.user_profiles.sum(axis=1)
            self.user_profiles = self.user_profiles.div(row_sums, axis=0).fillna(0)
            
            # Save profiles
            self.user_profiles.to_pickle(user_profiles_path)
            self.genre_cols = genre_cols
            
            return self.user_profiles, self.genre_cols
            
        except Exception as e:
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
            # Load movies in batches
            offset = 0
            all_movie_data = []
            
            while True:
                movies_df = self.db_manager.load_movies(limit=self.batch_size, offset=offset)
                if movies_df.empty or offset > 100000:
                    break
                    
                # Process genres
                movies_df["processed_genres"] = movies_df["genres"].apply(self.process_genres)
                all_movie_data.append(movies_df)
                
                offset += self.batch_size
                print(f"Processed {offset} movie records")
            
            # Combine all batches
            movies_df = pd.concat(all_movie_data, ignore_index=True)
            
            # Create genre columns
            for col in self.genre_cols:
                genre = col.replace("genre_", "")
                movies_df[col] = movies_df["processed_genres"].apply(lambda x: int(genre in x))
            
            # Create movie vectors
            self.movie_vectors = movies_df.set_index("movie_id")[self.genre_cols]
            
            # Normalize movie vectors
            row_sums = self.movie_vectors.sum(axis=1)
            self.movie_vectors = self.movie_vectors.div(row_sums, axis=0).fillna(0)
            
            # Save vectors
            self.movie_vectors.to_pickle(movie_vectors_path)
            
            return self.movie_vectors
            
        except Exception as e:
            raise RuntimeError(f"Error building movie genre vectors: {str(e)}")
    
    def train(self) -> Dict[str, Any]:
        """
        Train the content-based filtering model with improved processing.
        
        Returns:
            Dictionary containing the trained model
        """
        try:
            # Build user and movie profiles
            self.build_user_genre_profiles()
            self.build_movie_genre_vectors()
            
            # Calculate similarity matrix
            self.sim_matrix = cosine_similarity(self.movie_vectors)
            
            # Store model components
            self.model = {
                "genre_sim": self.sim_matrix,
                "movie_ids": self.movie_vectors.index.tolist(),
                "genre_mapping": self.genre_mapping
            }
            
            return self.model
            
        except Exception as e:
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
            if self.sim_matrix is None or self.movie_ids is None:
                raise ValueError("Model not trained. Call train() first.")
            
            # Find index of the movie
            try:
                movie_idx = self.movie_ids.index(movie_id)
            except ValueError:
                raise ValueError(f"Movie ID {movie_id} not found in training data")
            
            # Get similarity scores for the movie
            sim_scores = self.sim_matrix[movie_idx]
            
            # Get indices of top N similar movies (excluding self)
            similar_indices = np.argsort(sim_scores)[::-1][1:n_recommendations+1]
            
            # Convert indices to movie IDs
            recommended_ids = [self.movie_ids[idx] for idx in similar_indices]
            
            return recommended_ids
            
        except Exception as e:
            raise RuntimeError(f"Error getting recommendations: {str(e)}")