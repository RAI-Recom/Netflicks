import pickle
from config import USER_RECOMMENDATIONS_MODEL, POPULARITY_MODEL

# Load Precomputed Recommendations
def load_user_recommendations_model():
    try:
        with open(USER_RECOMMENDATIONS_MODEL, "rb") as f:
            return pickle.load(f)
    except FileNotFoundError:
        print('Error: User recommendation model not found')
        return {}


# Load Popularity-Based Model for Cold Start Users
def load_popularity_model():
    try:
        with open(POPULARITY_MODEL, "rb") as f:
            return pickle.load(f)
    except FileNotFoundError:
        print('Error: Popularity model not found')
        return []


def recommend_movies(user_id):
    popularity_model = load_popularity_model()
    user_recommendations = load_user_recommendations_model()
    
    if user_id in user_recommendations:
        return {"user_id": user_id, "recommendations": user_recommendations[user_id]}
    
    return {"user_id": user_id, "recommendations": popularity_model[:10]}