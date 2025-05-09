import sys
sys.path.append('.')

from flask import Flask, jsonify, Response, request
from loguru import logger
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
import time

from pipeline.hybrid_recommender import hybrid_recommend
import os
from dotenv import load_dotenv
import json

load_dotenv()

app = Flask(__name__)

# Prometheus Metrics
REQUEST_COUNT = Counter('recommendation_requests_total', 'Total recommendation requests received')
REQUEST_LATENCY = Histogram('recommendation_request_latency_seconds', 'Latency of recommendation requests in seconds')
REQUEST_ERRORS = Counter('recommendation_request_errors_total', 'Total number of failed recommendation requests')

@app.route('/recommend/<int:user_id>', methods=['GET'])
def recommend_movies(user_id):
    start_time = time.time()
    REQUEST_COUNT.inc()

    try:
        result = hybrid_recommend(user_id, 20)
        latency = time.time() - start_time
        REQUEST_LATENCY.observe(latency)
        return jsonify(result)

    except Exception as e:
        logger.error(e)
        REQUEST_ERRORS.inc()
        return Response('{"error": "Internal Server Error"}', status=500, content_type="application/json")

@app.errorhandler(400)
def bad_request(e):
    REQUEST_ERRORS.inc()
    return Response('{"error": "Invalid request. Please check the user ID."}', status=400, content_type="application/json")

@app.errorhandler(429)
def too_many_requests(e):
    REQUEST_ERRORS.inc()
    return Response('{"error": "Too Many Requests"}', status=429, content_type="application/json")

@app.errorhandler(503)
def service_unavailable(e):
    REQUEST_ERRORS.inc()
    return Response('{"error": "Service Unavailable"}', status=503, content_type="application/json")

# Prometheus metrics endpoint
@app.route('/metrics')
def metrics():
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)

def log_file_contents():
    commit_log_path = '/home/Recomm-project/datav/commit_log.txt'
    cb_config_path = '/home/Recomm-project/Netflicks/artifacts2/path/cb_artifact_config.json'
    popularity_config_path = '/home/Recomm-project/Netflicks/artifacts2/path/popularity_artifact_config.json'
    cf_config_path = '/home/Recomm-project/Netflicks/artifacts2/path/cf_artifact_config.json'

    # Log commit_log.txt
    try:
        with open(commit_log_path, 'r') as commit_file:
            commit_log = commit_file.read()
            logger.info("Contents of commit_log.txt:\n{}", commit_log)
    except Exception as e:
        logger.error("Failed to read commit_log.txt: {}", e)

    # Helper function to log artifact_uri
    def log_artifact_uri(file_path, label):
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                artifact_uri = data.get("artifact_uri", "Key 'artifact_uri' not found.")
                logger.info("{} artifact_uri: {}", label, artifact_uri)
        except Exception as e:
            logger.error("Failed to read or parse {}: {}", label, e)

    # Log all artifact config files
    log_artifact_uri(cb_config_path, "CB Config")
    log_artifact_uri(popularity_config_path, "Popularity Config")
    log_artifact_uri(cf_config_path, "CF Config")




if __name__ == '__main__':
    logger.add("api.log")
    log_file_contents()
    app.run(host='0.0.0.0', port=os.getenv('API_PORT', '5000'))
