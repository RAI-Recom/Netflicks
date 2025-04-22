from sklearn.metrics.pairwise import cosine_similarity
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any, Optional


class ContentBasedFiltering:
    """
    A class for content-based filtering recommendation system.
    """
    
    def __init__(self):
        """Initialize the ContentBasedFiltering model."""
        self.genre_cols = []
        self.genre_matrix = None
        self.sim_matrix = None
        self.movie_ids = []
        self.model = {}
        
    def assign_watch_weight(self, minutes: float) -> float:
        """
        Assign a weight to a watch session based on minutes watched.
        
        Args:
            minutes: Number of minutes watched
            
        Returns:
            Weight value between -0.5 and 1.0
        """
        if pd.isna(minutes): 
            return 0.0
        if minutes >= 80: 
            return 1.0
        elif minutes >= 40: 
            return 0.5
        elif minutes <= 20: 
            return -0.5
        return 0.0
    
    def build_user_genre_profiles(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """
        Build user genre profiles from watch history.
        
        Args:
            df: DataFrame containing watch history with 'genres' and 'watched_minutes' columns
            
        Returns:
            Tuple of (user profiles DataFrame, list of genre column names)
        """
        
        # Extract all unique genres
        all_genres = set(g for genres in df["genres"] for g in genres)
        genre_cols = []
        
        # Create one-hot encoded columns for each genre
        for genre in all_genres:
            col = f"genre_{genre.strip().replace(' ', '_').lower()}"
            df[col] = df["genres"].apply(lambda x: int(genre in x))
            genre_cols.append(col)
        
        # Apply watch weight
        df["watch_weight"] = df["watched_minutes"].apply(self.assign_watch_weight)
        
        # Weight genre columns by watch weight
        for col in genre_cols:
            df[col] = df[col] * df["watch_weight"]
        
        # Group by user and sum weighted vectors
        user_profiles = df.groupby("user_id")[genre_cols].sum()
        
        self.genre_cols = genre_cols
        return user_profiles, genre_cols
    
    def one_hot_encode_genres(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        One-hot encode genres in the DataFrame.
        
        Args:
            df: DataFrame containing 'genres' column
            
        Returns:
            DataFrame with one-hot encoded genre columns
        """
        # Convert genres string to list
        df["genres"] = df["genres"].apply(
            lambda x: x.split() if isinstance(x, str) else []
        )
        
        # Extract all unique genres
        all_genres = set(g for genres in df["genres"] for g in genres)
        
        # Create one-hot encoded columns for each genre
        for genre in all_genres:
            col = f"genre_{genre.strip().replace(' ', '_').lower()}"
            df[col] = df["genres"].apply(lambda x: int(genre in x))
            
        self.genre_cols = [col for col in df.columns if col.startswith("genre_")]
        return df
    
    def train(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Train the content-based filtering model.
        
        Args:
            df: DataFrame containing movie data with 'genres' column
            
        Returns:
            Dictionary containing the trained model
        """
        # One-hot encode genres
        df = self.one_hot_encode_genres(df)
        
        # Extract genre matrix
        genre_matrix = df[df.columns[df.columns.str.startswith("genre_")]]
        
        # Calculate similarity matrix
        sim_matrix = cosine_similarity(genre_matrix)
        
        # Store model components
        self.genre_matrix = genre_matrix
        self.sim_matrix = sim_matrix
        self.movie_ids = df["movie_id"].tolist()
        
        # Create model dictionary
        self.model = {
            "genre_sim": sim_matrix,
            "movie_ids": self.movie_ids
        }
        
        return self.model
    
    def get_recommendations(self, movie_id: int, n_recommendations: int = 5) -> List[int]:
        """
        Get movie recommendations based on content similarity.
        
        Args:
            movie_id: ID of the movie to base recommendations on
            n_recommendations: Number of recommendations to return
            
        Returns:
            List of recommended movie IDs
        """
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


def build_user_genre_profiles(df):
    model = ContentBasedFiltering()
    return model.build_user_genre_profiles(df)