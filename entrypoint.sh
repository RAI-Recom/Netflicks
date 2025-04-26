#!/bin/bash

# Start mlflow server in the background
mlflow server \
     --backend-store-uri sqlite:///mlflow.db \
     --default-artifact-root /app/mlruns \
     --host 0.0.0.0 \
     --port 6001 &

# Wait a bit to make sure mlflow has started (optional but good practice)
sleep 5

# Start the training pipeline
python pipeline/training_pipeline.py
