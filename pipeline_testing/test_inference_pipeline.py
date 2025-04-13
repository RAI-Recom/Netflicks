import os
import pandas as pd
import pickle

def test_model_exists():
    assert os.path.exists("models/popular_movies.pkl"), "Model file not found."

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

if __name__ == "__main__":
    test_model_exists()
    test_model_can_be_loaded()
    test_inference_output_exists()
    test_inference_output_valid()
    print("✅ All inference tests passed.")
