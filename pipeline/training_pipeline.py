import pickle
from db.db_manager import DBManager
from pipeline.model_pipeline.collaborative_filtering import CollaborativeFiltering
from pipeline.model_pipeline.popularity_model import PopularityModel
from pipeline.model_pipeline.content_based_filtering import ContentBasedFiltering


class TrainingPipeline:
    """
    A class that encapsulates the entire training pipeline for recommendation models.
    """
    
    def __init__(self, ratings_limit=10000, ratings_offset=0, watch_limit=10000, watch_offset=0):
        """
        Initialize the training pipeline.
        
        Args:
            ratings_limit: Limit for ratings data loading
            ratings_offset: Offset for ratings data loading
            watch_limit: Limit for watch history data loading
            watch_offset: Offset for watch history data loading
        """
        self.ratings_limit = ratings_limit
        self.ratings_offset = ratings_offset
        self.watch_limit = watch_limit
        self.watch_offset = watch_offset
        self.db_manager = DBManager()
        self.watch_df = self.db_manager.load_watch_chunk(limit=self.watch_limit, offset=self.watch_offset)
        self.content_based_filtering = ContentBasedFiltering(self.watch_df)
        
    def load_data(self):
        """Load all necessary data for training."""
        return self
    
    def train_popularity_model(self):
        """Train and save the popularity model."""
        popularity_model = PopularityModel()
        self.movies_df = self.db_manager.get_movie_ratings_and_votes()
        popularity_model.train_and_save(self.movies_df)
        return self
    
    def build_profiles(self):
        """Build and save user and movie profiles."""
        self.content_based_filtering.build_user_genre_profiles()
        self.content_based_filtering.build_movie_genre_vectors()
        return self
    
    def train_content_based_filtering_model(self):
        """Train and save content-based model."""
        self.watch_df["genres"] = self.watch_df["genres"].apply(lambda g: g if isinstance(g, list) else [])
        cb_model = self.content_based_filtering.train()
        self.save_model(cb_model, "models/cb_model.pkl")
        return self
    
    def train_collaborative_filtering_model(self):
        """Train and save collaborative filtering model."""
        ratings_df = self.db_manager.load_ratings_chunk(limit=self.ratings_limit, offset=self.ratings_offset)
        ratings_df = ratings_df.dropna(subset=["rating"]).copy()
        cf_model = CollaborativeFiltering(n_components=50)
        cf_model.train(ratings_df)
        model_info = cf_model.get_model_info()
        self.save_model(model_info, "models/cf_model.pkl")
        return self
    
    def save_model(self, obj, path):
        with open(path, "wb") as f:
            pickle.dump(obj, f)
        return self
    
    def run(self):
        """Run the entire training pipeline."""
        try:
            print("Starting the training pipeline...")
            self.load_data()
            print("Data loaded successfully.")
            
            self.train_popularity_model()
            print("Popularity model trained successfully.")
            
            self.build_profiles()
            print("Profiles built successfully.")
            
            self.train_content_based_filtering_model()
            print("Content-based filtering model trained successfully.")
            
            self.train_collaborative_filtering_model()
            print("Collaborative filtering model trained successfully.")
            
            print("✅ Training pipeline completed successfully.")
        except Exception as e:
            print(f"Error during training pipeline execution: {str(e)}")
        return self


if __name__ == "__main__":
    pipeline = TrainingPipeline()
    pipeline.run()
