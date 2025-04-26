#!/bin/bash

# Start mlflow server in the background
mlflow ui --host 0.0.0.0 --port 6001 --backend-store-uri file:./mlruns &

# Wait a bit to make sure mlflow has started (optional but good practice)
sleep 5

# Start the training pipeline
python pipeline/training_pipeline.py
