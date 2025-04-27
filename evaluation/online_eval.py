from collections import defaultdict
import numpy as np

class OnlineEvaluator:
    def __init__(self, k: int = 20, default_rating: float = 3.0):
        self.k = k
        self.default_rating = default_rating
        self.user_last_recommendations = defaultdict(list)
        self.actual_ratings = []
        self.predicted_ratings = []
        self.hits = 0
        self.total_users = 0

    def process_recommendation(self, user_id: int, recommended_movie_ids: list):
        self.user_last_recommendations[user_id] = recommended_movie_ids[:self.k]

    def process_watch(self, user_id: int, movie_id: int):
        if movie_id in self.user_last_recommendations.get(user_id, []):
            self.hits += 1
        self.total_users += 1

    def process_rating(self, user_id: int, movie_id: int, rating: float):
        pred = self.default_rating
        self.predicted_ratings.append(pred)
        self.actual_ratings.append(rating)

    def compute_metrics(self):
        hitrate = self.hits / self.total_users if self.total_users > 0 else 0.0
        if self.actual_ratings:
            rmse = np.sqrt(np.mean((np.array(self.actual_ratings) - np.array(self.predicted_ratings)) ** 2))
        else:
            rmse = None
        return hitrate, rmse
