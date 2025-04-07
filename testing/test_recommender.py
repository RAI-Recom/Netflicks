import pickle
import pandas as pd
import time
import os
import random

# Load model from .pkl file
def load_model(model_path):
    with open(model_path, "rb") as file:
        model = pickle.load(file)
    print(type(model))  # Should NOT be <class 'list'>
    #print(model)
    return model

# Efficiently sample 100 users from a large CSV file
def load_sampled_test_data(test_data_path, sample_size=500, chunk_size=50000):
    """Loads a random subset of 100 users efficiently using chunking."""
    sampled_users = set()
    sampled_data = []

    for chunk in pd.read_csv(test_data_path, chunksize=chunk_size, usecols=["user_id", "movie_title"]):
        unique_users = chunk["user_id"].unique()
        new_users = list(set(unique_users) - sampled_users)

        # Select some new users from the current chunk
        if len(sampled_users) < sample_size:
            sampled_users.update(new_users[:max(0, sample_size - len(sampled_users))])
            sampled_data.append(chunk[chunk["user_id"].isin(sampled_users)])
        
        if len(sampled_users) >= sample_size:
            break

    return pd.concat(sampled_data, ignore_index=True)

# Generate recommendations (Assumes model has a 'recommend' function)
def get_recommendations(model, user_ids, K=10):
    """Generate top-K recommendations for each user, handling list- and dict-based models."""
    recommendations = {}

    if isinstance(model, list):  # If model is a list, return top-K movies for all users
        for user in user_ids:
            recommendations[user] = model[:K]  # Same recommendations for all users

    elif isinstance(model, dict):  # If model is a dictionary, return user-specific recommendations
        for user in user_ids:
            recommendations[user] = model.get(user, [])[:K]  # Return top-K or empty if user not in dict

    elif hasattr(model, "recommend"):  # If model has a recommend() method, use it
        for user in user_ids:
            recommendations[user] = model.recommend(user, K)

    else:
        raise TypeError("Model format is not supported. Expected a list, dict, or model with a 'recommend' method.")

    return recommendations



# Compute HR@K
def hit_rate_at_k(recommendations, test_data, K=10):
    """HR@K = (Number of users with at least one recommended movie in their actual test set) / (Total users)."""
    user_watched = test_data.groupby("user_id")["movie_title"].apply(set).to_dict()

    hits = 0
    total_users = len(recommendations)
    #print("Sanjana",total_users)

    for user, rec_movies in recommendations.items():
        if user in user_watched and any(movie in user_watched[user] for movie in rec_movies):
            hits += 1

    hr_k = hits / total_users #if total_users > 0 else 0
    return hr_k

def precision_at_k(recommendations, test_data, K=10):
    user_watched = test_data.groupby("user_id")["movie_title"].apply(set).to_dict()

    precision_scores = []
    for user, rec_movies in recommendations.items():
        if user in user_watched:
            hits = sum(1 for movie in rec_movies if movie in user_watched[user])
            precision_scores.append(hits / K)  # Fraction of recommended movies that match watched ones

    return sum(precision_scores) / len(precision_scores) if precision_scores else 0



# Measure inference cost (average recommendation time per user)
def measure_inference_time(model, user_ids, K=10):
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
    test_data_path = "../data/processed_rate_entries.csv"
    
    print("Loading models...")
    popularity_model = load_model(popularity_model_path)
    item_to_item_model = load_model(item_to_item_model_path)

    print("Loading sampled test data (Only 100 Users)...")
    test_data = load_sampled_test_data(test_data_path, sample_size=100)
    user_ids = test_data["user_id"].unique()

    model_users = set(item_to_item_model.keys())

    # Extract users from test data
    test_users = set(user_ids)  # user_ids comes from test_data["user_id"].unique()

    # Find missing users
    missing_users = test_users - model_users
    # Debug output
    print(f"Total test users: {len(test_users)}")
    print(f"Users in model: {len(model_users)}")
    print(f"Users missing in model: {len(missing_users)}")  # If high, users are being skipped
    print(f"Example missing users: {list(missing_users)[:10]}")

        
    print(f"Testing on {len(user_ids)} users.")

    # HR@K Evaluation
    print("Evaluating Popularity-Based Model...")
    pop_recommendations = get_recommendations(popularity_model, user_ids, K=10)
    
    pop_hr_k = hit_rate_at_k(pop_recommendations, test_data, K=10)

    print("Evaluating Item-to-Item Model...")
    item_recommendations = get_recommendations(item_to_item_model, user_ids, K=10)
    item_hr_k = hit_rate_at_k(item_recommendations, test_data, K=10)

    empty_recs = [user for user, recs in item_recommendations.items() if not recs]
    print(f"Users with empty recommendations: {len(empty_recs)} ")


    # Measure Inference Time
    print("Measuring inference time...")
    pop_inference_time = measure_inference_time(popularity_model, user_ids, K=10)
    item_inference_time = measure_inference_time(item_to_item_model, user_ids, K=10)

    # Model Sizes
    pop_model_size = get_model_size(popularity_model_path)
    item_model_size = get_model_size(item_to_item_model_path)

    # Print results
    print("\n=== Model Comparison Results ===")
    print(f"{'Metric':<25}{'Popularity-Based':<20}{'Item-to-Item'}")
    print("-" * 60)
    print(f"{'Precisions (%)':<25}{pop_hr_k*100:.2f}{' ' * 10}{item_hr_k*100:.2f}")
    print(f"{'Inference Time (ms/user)':<25}{pop_inference_time:.2f}{' ' * 10}{item_inference_time:.2f}")
    print(f"{'Model Size (MB)':<25}{pop_model_size:.2f}{' ' * 10}{item_model_size:.2f}")

if __name__ == "__main__":
    main()
