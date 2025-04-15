import pickle

def save_model(obj, path):
    with open(path, "wb") as f:
        pickle.dump(obj, f)