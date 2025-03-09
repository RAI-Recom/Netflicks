import pickle
import pandas as pd
import numpy as np
import time
import os

# Load model from .pkl file
def load_model(model_path):
    with open(model_path, "rb") as file:
        model = pickle.load(file)
    return model

# Load test dataset (CSV format)
def load_test_data(test_data_path):
    """Assumes test data contains columns: user_id, movie_title, rating"""
    return pd.read_csv(test_data_path)

# Generate recommendations (Assumes model has a 'recommend' function)
def get_recommendations(model, user_ids, K=20):
    """
    Generate top-K recommendations for each user.
    Assumes the model has a `recommend(user_id, K)` method.
    """
    recommendations = {}
    for user in user_ids:
        try:
            recommendations[user] = model.recommend(user, K)  # Ensure model has recommend() function
        except AttributeError:
            print(f"Error: Model does not have a recommend(user, K) function. Modify the script accordingly.")
            return None
    return recommendations

# Compute HR@K
def hit_rate_at_k(recommendations, test_data, K=20):
    """
    HR@K = (Number of users with at least one recommended movie in their actual test set) / (Total users)
    """
    user_watched = test_data.groupby("user_id")["movie_title"].apply(set).to_dict()
    
    hits = 0
    total_users = len(recommendations)
    
    for user, rec_movies in recommendations.items():
        if user in user_watched and any(movie in user_watched[user] for movie in rec_movies):
            hits += 1
    
    hr_k = hits / total_users if total_users > 0 else 0
    return hr_k

# Measure inference cost (average recommendation time per user)
def measure_inference_time(model, user_ids, K=20):
    start_time = time.time()
    get_recommendations(model, user_ids, K)
    end_time = time.time()
    return (end_time - start_time) / len(user_ids) * 1000  # Convert to milliseconds per user

# Get model size in MB
def get_model_size(model_path):
    return os.path.getsize(model_path) / (1024 * 1024)  # Convert bytes to MB

# Main function to compare models
def main():
    # Define model and data paths
    popularity_model_path = "../models/popular_movies.pkl"
    item_to_item_model_path = "../models/user_recommendations.pkl"
    test_data_path = "../data/processed_data_entries.csv"
    
    print("Loading models...")
    popularity_model = load_model(popularity_model_path)
    item_to_item_model = load_model(item_to_item_model_path)

    print("Loading test data...")
    test_data = load_test_data(test_data_path)
    user_ids = test_data["user_id"].unique()
    
    # HR@K Evaluation
    print("Evaluating Popularity-Based Model...")
    pop_recommendations = get_recommendations(popularity_model, user_ids, K=20)
    pop_hr_k = hit_rate_at_k(pop_recommendations, test_data, K=20)
    
    print("Evaluating Item-to-Item Model...")
    item_recommendations = get_recommendations(item_to_item_model, user_ids, K=20)
    item_hr_k = hit_rate_at_k(item_recommendations, test_data, K=20)

    # Measure Inference Time
    print("Measuring inference time...")
    pop_inference_time = measure_inference_time(popularity_model, user_ids, K=20)
    item_inference_time = measure_inference_time(item_to_item_model, user_ids, K=20)

    # Model Sizes
    pop_model_size = get_model_size(popularity_model_path)
    item_model_size = get_model_size(item_to_item_model_path)

    # Print results
    print("\n=== Model Comparison Results ===")
    print(f"{'Metric':<25}{'Popularity-Based':<20}{'Item-to-Item'}")
    print("-" * 60)
    print(f"{'HR@10 (%)':<25}{pop_hr_k*100:.2f}{' ' * 10}{item_hr_k*100:.2f}")
    print(f"{'Inference Time (ms/user)':<25}{pop_inference_time:.2f}{' ' * 10}{item_inference_time:.2f}")
    print(f"{'Model Size (MB)':<25}{pop_model_size:.2f}{' ' * 10}{item_model_size:.2f}")

if __name__ == "__main__":
    main()
