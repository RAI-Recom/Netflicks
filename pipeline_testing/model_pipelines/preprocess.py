import pandas as pd

def preprocess_collaborative_filtering_model(df):
    # Keep only rows with valid numeric ratings
    return df.dropna(subset=["rating"]).copy()

def preprocess_content_based_model(df):
    # Convert list of genres to space-separated string for vectorization
    # df["plot"] = df["plot"].fillna("")
    # df["combined_text"] = df["genres"] + " " + df["plot"]
    return df
