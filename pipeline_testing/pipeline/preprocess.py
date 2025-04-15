import pandas as pd

def preprocess_cf(df):
    # Keep only rows with valid numeric ratings
    return df.dropna(subset=["rating"]).copy()

def preprocess_cb(df):
    # Convert list of genres to space-separated string for vectorization
    df["genres"] = df["genres"].apply(lambda g: " ".join(g) if isinstance(g, list) else "")
    df["plot"] = df["plot"].fillna("")
    df["combined_text"] = df["genres"] + " " + df["plot"]
    return df
