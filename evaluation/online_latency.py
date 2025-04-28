import sys
sys.path.append('.')

import os
import time
import json
import requests
import pandas as pd
import numpy as np
from tqdm import tqdm
from db.db_manager import DBManager
from sklearn.metrics import mean_squared_error

# --- CONFIGURATION ---
api_port = os.getenv('API_PORT', '5000')
API_ENDPOINT = f"http://localhost:{api_port}/recommend"
 # Change if different
MAX_USERS = 10000  # Number of users to sample
K = 20  # Number of recommendations to ask for
TIMEOUT = 5  # seconds per API request
EVAL_OUTPUT_PATH = "evaluation/metrics_online.json"
DEFAULT_PRED_RATING = 3.0  # if needed later for RMSE

# --- FETCH USERS ---
print("Connecting to database...")
db_manager = DBManager()
ratings_df = db_manager.load_ratings_chunk(limit=10000, offset=0)

print(f"Loaded {len(ratings_df)} ratings")

#user_ids = ratings_df["user_id"].drop_duplicates().sample(n=MAX_USERS, random_state=42).tolist()
user_pool = ratings_df["user_id"].drop_duplicates()
n_users = min(MAX_USERS, len(user_pool))

user_ids = user_pool.sample(n=n_users, random_state=42).tolist()

print(f"Selected {len(user_ids)} random users for evaluation")

# --- METRICS STORAGE ---
latencies = []
statuses = []
recommendation_counts = []

# --- API Evaluation ---
print("Starting API evaluation...")
for user_id in tqdm(user_ids, desc="Querying API"):
    try:
        start_time = time.time()
        response = requests.get(f"{API_ENDPOINT}/{user_id}")
        latency = time.time() - start_time

        latencies.append(latency)
        statuses.append(response.status_code)

        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list):
                recommendation_counts.append(len(data))
            else:
                recommendation_counts.append(0)
        else:
            recommendation_counts.append(0)

    except requests.exceptions.RequestException:
        latencies.append(TIMEOUT)
        statuses.append(500)  # simulate failure
        recommendation_counts.append(0)

# --- METRICS COMPUTATION ---

latencies_ms = [l * 1000 for l in latencies]  # convert to ms

metrics = {}

# Success/Error Rates
total_requests = len(statuses)
success_count = statuses.count(200)
error_count = total_requests - success_count
metrics["success_rate"] = round(success_count / total_requests, 4)
metrics["error_rate"] = round(error_count / total_requests, 4)

# Latency
metrics["mean_latency_ms"] = round(np.mean(latencies_ms), 2)
metrics["median_latency_ms"] = round(np.percentile(latencies_ms, 50), 2)
metrics["p95_latency_ms"] = round(np.percentile(latencies_ms, 95), 2)
metrics["p99_latency_ms"] = round(np.percentile(latencies_ms, 99), 2)
metrics["max_latency_ms"] = round(np.max(latencies_ms), 2)
metrics["latency_stddev_ms"] = round(np.std(latencies_ms), 2)

# Throughput
total_time_sec = sum(latencies)
metrics["throughput_rps"] = round(total_requests / total_time_sec, 2)

# SLA violation rate (optional, assume SLA = 300ms)
sla_threshold_ms = 300
sla_violations = [1 for l in latencies_ms if l > sla_threshold_ms]
metrics["sla_violation_rate"] = round(len(sla_violations) / total_requests, 4)

# Recommendation counts
metrics["mean_recommendations_per_user"] = round(np.mean(recommendation_counts), 2)

# --- SAVE RESULTS ---

print("\n=== Final Online Evaluation Metrics ===")
for k, v in metrics.items():
    print(f"{k}: {v}")

os.makedirs(os.path.dirname(EVAL_OUTPUT_PATH), exist_ok=True)
with open(EVAL_OUTPUT_PATH, "w") as f:
    json.dump(metrics, f, indent=2)

print(f"\n✅ Metrics saved to {EVAL_OUTPUT_PATH}")
