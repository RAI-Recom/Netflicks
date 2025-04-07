import time
import requests
import pickle

# API Endpoint
API_URL = "http://localhost:8082/recommend/{}"  # Update with correct API URL

# Load Popularity-Based Model
popularity_model_path = "../models/popular_movies.pkl"
with open(popularity_model_path, "rb") as f:
    popular_movies = pickle.load(f)  # List of (movie, score) tuples

# Define test users (subset for benchmarking)
#test_users = list(watched_movies_per_user.keys())[:100]  # Testing on 100 users
#test_users = 

# Function to time inference for API-based recommendations
def time_api_recommendations(users):
    start_time = time.time()
    recommendations = {}

    for user in users:
        try:
            response = requests.get(API_URL.format(user), timeout=5)
            print(f"User {user} Response: {response.status_code}, {response.text}")  # Debugging line

            if response.status_code == 200:
                json_response = response.json()
                
                
                if isinstance(json_response, list):
                    recommendations[user] = json_response  # Response is a list of movies
                else:
                    recommendations[user] = json_response.get("recommended_movies", []) 
                
                #print(f"User {user} Recommendations: {recommendations[user]}")  # Debugging line
            else:
                recommendations[user] = []
        except requests.exceptions.RequestException as e:
            print(f"Request Exception for User {user}: {e}")
            recommendations[user] = []

    total_time = time.time() - start_time
    avg_time_per_user = total_time / len(users)
    print(f"API-Based Model: Total Inference Time = {total_time:.4f} sec, Avg per user = {avg_time_per_user:.4f} sec")

# Function to time inference for Popularity-Based Model
def time_popularity_recommendations(users, top_n=10):
    start_time = time.time()
    
    recommendations = {user: [movie for movie, _ in popular_movies[:top_n]] for user in users}
    
    total_time = time.time() - start_time
    avg_time_per_user = total_time / len(users)
    print(f"Popularity-Based Model: Total Inference Time = {total_time:.4f} sec, Avg per user = {avg_time_per_user:.6f} sec")

# Measure API Inference Time
time_api_recommendations(test_users)

# Measure Popularity Model Inference Time
time_popularity_recommendations(test_users)
