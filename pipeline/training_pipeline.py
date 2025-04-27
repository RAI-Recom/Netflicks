import sys
sys.path.append('.')

import os
import logging
import pickle
from typing import Dict, Any, Optional
from pathlib import Path
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from db.db_manager import DBManager
from pipeline.model_pipeline.collaborative_filtering import CollaborativeFiltering
from pipeline.model_pipeline.popularity_model import PopularityModel
from pipeline.model_pipeline.content_based_filtering import ContentBasedFiltering

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TrainingPipeline:
    """
    A class that encapsulates the entire training pipeline for recommendation models.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the training pipeline.
        
        Args:
            config: Configuration dictionary containing:
                - watch_limit: Limit for watch history data loading
                - watch_offset: Offset for watch history data loading
                - model_paths: Dictionary of model save paths
                - n_components: Number of components for CF model
                - batch_size: Batch size for processing
        """
        self.config = config
        self.db_manager = None
        self.watch_df = None
        self.content_based_filtering = None
        self._setup_directories()
        
    def _setup_directories(self) -> None:
        """Create necessary directories for model storage."""
        try:
            model_dir = Path("models")
            model_dir.mkdir(exist_ok=True)
            logger.info("Model directory setup complete")
        except Exception as e:
            logger.error(f"Error setting up directories: {str(e)}")
            raise
            
    def _validate_data(self, df: pd.DataFrame, required_columns: list[str]) -> bool:
        """
        Validate the loaded data.
        
        Args:
            df: DataFrame to validate
            required_columns: List of required column names
            
        Returns:
            bool: True if validation passes
        """
        try:
            # Check for required columns
            missing_cols = [col for col in required_columns if col not in df.columns]
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")
                
            # Check for empty DataFrame
            if df.empty:
                raise ValueError("Empty DataFrame")
                
            # Check for null values in critical columns
            null_counts = df[required_columns].isnull().sum()
            if null_counts.any():
                logger.warning(f"Null values found in columns: {null_counts[null_counts > 0]}")
                
            return True
            
        except Exception as e:
            logger.error(f"Data validation failed: {str(e)}")
            return False
            
    def load_data(self) -> 'TrainingPipeline':
        """Load all necessary data for training."""
        try:
            logger.info("Connecting to database...")
            self.db_manager = DBManager()
            
            # Initialize content-based filtering
            self.content_based_filtering = ContentBasedFiltering(
                batch_size=self.config.get("batch_size", 10000)
            )
            
            logger.info("Data loading complete")
            return self
            
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            raise
            
    def train_popularity_model(self) -> 'TrainingPipeline':
        """Train and save the popularity model."""
        try:
            logger.info("Training popularity model...")
            popularity_model = PopularityModel()
            
            # Load and validate movie data
            self.movies_df = self.db_manager.get_movie_ratings_and_votes()
            if not self._validate_data(self.movies_df, ["movie_id", "rating", "votes"]):
                raise ValueError("Movie data validation failed")
                
            # Train and save model
            popularity_model.train_and_save(self.movies_df)
            logger.info("Popularity model training complete")
            return self
            
        except Exception as e:
            logger.error(f"Error training popularity model: {str(e)}")
            raise
            
    def train_content_based_filtering_model(self) -> 'TrainingPipeline':
        """Train and save content-based model."""
        try:
            logger.info("Training content-based filtering model...")
            
            # Train model
            cb_model = self.content_based_filtering.train()
            
            # Validate model
            if not isinstance(cb_model, dict) or "movie_vectors" not in cb_model:
                raise ValueError("Invalid content-based model format")
                
            # Save model
            self.save_model(cb_model, self.config["model_paths"]["cb_model"])
            
            logger.info("Content-based filtering model training complete")
            return self
            
        except Exception as e:
            logger.error(f"Error training content-based model: {str(e)}")
            raise
            
    def train_collaborative_filtering_model(self) -> 'TrainingPipeline':
        """Train and save collaborative filtering model."""
        try:
            logger.info("Training collaborative filtering model...")
            
            # Initialize and train model
            cf_model = CollaborativeFiltering(
                n_components=self.config["n_components"]
            )
            cf_model.train()
            
            # Get and validate model info
            model_info = cf_model.get_model_info()
            if not isinstance(model_info, dict) or "user_factors" not in model_info:
                raise ValueError("Invalid collaborative filtering model format")
                
            # Save model
            self.save_model(model_info, self.config["model_paths"]["cf_model"])
            cf_model.log_model_to_mlflow(self)
            logger.info("Collaborative filtering model training complete")
            return self
            
        except Exception as e:
            logger.error(f"Error training collaborative filtering model: {str(e)}")
            raise
            
    def save_model(self, obj: Any, path: str) -> None:
        """
        Save a model object to disk.
        
        Args:
            obj: Model object to save
            path: Path to save the model
        """
        try:
            with open(path, "wb") as f:
                pickle.dump(obj, f)
            logger.info(f"Model saved to {path}")
        except Exception as e:
            logger.error(f"Error saving model to {path}: {str(e)}")
            raise
            
    def run(self) -> 'TrainingPipeline':
        """Run the entire training pipeline."""
        try:
            logger.info("Starting training pipeline...")
            
            # Load data
            self.load_data()
            
            # Train models sequentially
            logger.info("Training popularity model...")
            self.train_popularity_model()
            
            logger.info("Training collaborative filtering model...")
            self.train_collaborative_filtering_model()
            
            logger.info("Training content-based filtering model...")
            self.train_content_based_filtering_model()
            
            logger.info("✅ Training pipeline completed successfully")
            return self
            
        except Exception as e:
            logger.error(f"Error during training pipeline execution: {str(e)}")
            raise

if __name__ == "__main__":
    # Configuration
    config = {
        "watch_limit": 10000,
        "watch_offset": 0,
        "model_paths": {
            "cb_model": "models/cb_model.pkl",
            "cf_model": "models/cf_model.pkl"
        },
        "n_components": 50,
        "batch_size": 1000
    }
    
    # Run pipeline
    pipeline = TrainingPipeline(config)
    pipeline.run()
