import sys
sys.path.append('.')

from flask import Flask, jsonify, Response, request
from loguru import logger
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
import time

from pipeline.hybrid_recommender import hybrid_recommend
import os
from dotenv import load_dotenv
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

if __name__ == '__main__':
    logger.add("api.log")
    app.run(host='0.0.0.0', port=os.getenv('API_PORT', '5000'))
