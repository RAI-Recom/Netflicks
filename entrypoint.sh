#!/bin/bash

# Start mlflow server in the background
mlflow server \
     --backend-store-uri sqlite:///mlflow.db \
     --default-artifact-root /app/mlruns \
     --host 127.0.0.1 \
     --port 6001 &

# Wait a bit to make sure mlflow has started (optional but good practice)
sleep 5

# Start the training pipeline
python pipeline/training_pipeline.py
