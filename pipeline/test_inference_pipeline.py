import os
import pandas as pd
import pickle
# import requests
# import json
# import sys

def test_model_can_be_loaded():
    with open("models/popular_movies.pkl", "rb") as f:
        model = pickle.load(f)
    assert isinstance(model, list), "Model format unexpected. Expected a list of popular movies."

def test_inference_output_exists():
    assert os.path.exists("output/user_recommendations.csv"), "Inference output file not found."

def test_inference_output_valid():
    df = pd.read_csv("output/user_recommendations.csv")
    assert not df.empty, "Recommendation output is empty."
    assert "user_id" in df.columns and "movie_id" in df.columns, "Required columns missing in output."



# def test_recommendations():
#     print("Calling FastAPI endpoint...")

#     try:
#         response = requests.get("http://localhost:8082/recommend/2")
#         response.raise_for_status()
#     except requests.RequestException as e:
#         print(f"Test failed: Could not reach API. Error: {e}")
#         sys.exit(1)

#     try:
#         data = response.json()
#     except json.JSONDecodeError:
#         print("Test failed: Invalid JSON response from API.")
#         sys.exit(1)

#     with open("response.json", "w") as f:
#         json.dump(data, f, indent=2)

#     count = len(data)
#     with open("count.txt", "w") as f:
#         f.write(str(count))

#     if count == 20:
#         print("Test passed: API returned 20 movie recommendations.")
#     else:
#         print(f"Test failed: Expected 20 recommendations, got {count}.")

if __name__ == "__main__":
    test_model_can_be_loaded()
    test_inference_output_exists()
    test_inference_output_valid()
    # test_recommendations()
    print("✅ All inference tests passed.")
