import pickle
import numpy as np
from surprise import SVD
from sklearn.metrics.pairwise import cosine_similarity

def load_models():
    with open("models/svd_cf_model.pkl", "rb") as f:
        cf_model = pickle.load(f)
    with open("models/popular_movies.pkl", "rb") as f:
        popularity_model = pickle.load(f)
    with open("models/user_profiles.pkl", "rb") as f:
        user_profiles = pickle.load(f)
    with open("models/movie_profiles.pkl", "rb") as f:
        movie_profiles = pickle.load(f)
    return cf_model,popularity_model, user_profiles, movie_profiles

def hybrid_recommend(user_id, cf_model, user_profiles, movie_profiles, movie_list, alpha=0.7, beta=0.3, top_n=5):
    recommendations = []
    user_profile = user_profiles.get(user_id, np.zeros(len(next(iter(movie_profiles.values())))))

    for movie_id in movie_list:
        # CF prediction
        try:
            cf_score = cf_model.predict(user_id, movie_id).est
        except:
            cf_score = 0.0

        # Content similarity
        content_vector = movie_profiles.get(movie_id, np.zeros_like(user_profile))
        content_score = cosine_similarity([user_profile], [content_vector])[0][0] if np.linalg.norm(content_vector) != 0 else 0.0

        final_score = alpha * cf_score + beta * content_score
        recommendations.append((movie_id, final_score))

    recommendations.sort(key=lambda x: x[1], reverse=True)
    return [movie_id for movie_id, _ in recommendations[:top_n]]