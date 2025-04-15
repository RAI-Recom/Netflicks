import sys
sys.path.append('.')

from flask import Flask, jsonify, Response
from loguru import logger
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from model_training.model_service import load_popularity_model, load_user_recommendations_model
import time

app = Flask(__name__)

# Load models clearly on startup
popularity_model = load_popularity_model()
user_recommendations = load_user_recommendations_model()

# Prometheus Metrics
REQUEST_COUNT = Counter('movie_recommendation_requests_total', 'Total recommendation requests')
REQUEST_ERRORS = Counter('movie_recommendation_request_errors_total', 'Total recommendation errors')
REQUEST_LATENCY = Histogram('movie_recommendation_request_latency_seconds', 'Latency of recommendation requests')

@app.route('/recommend/<int:user_id>', methods=['GET'])
def recommend_movies(user_id):
    start_time = time.time()
    REQUEST_COUNT.inc()
    try:
        if user_id in user_recommendations:
            logger.info(f"Recommendation served from user_recommendations for user {user_id}.")
            response = jsonify(user_recommendations[user_id])
        else:
            logger.info(f"Recommendation served from popularity_model for user {user_id}.")
            movie_ids = [movie[0] for movie in popularity_model[:20]]
            response = jsonify(movie_ids)

        latency = time.time() - start_time
        REQUEST_LATENCY.observe(latency)

        return response

    except Exception as e:
        logger.exception("Error during recommendation request.")
        REQUEST_ERRORS.inc()
        return Response('{"error": "Internal Server Error"}', status=500, content_type="application/json")

@app.errorhandler(400)
def bad_request(e):
    REQUEST_ERRORS.inc()
    return Response('{"error": "Invalid request."}', status=400, content_type="application/json")

@app.errorhandler(429)
def too_many_requests(e):
    REQUEST_ERRORS.inc()
    return Response('{"error": "Too Many Requests"}', status=429, content_type="application/json")

@app.errorhandler(503)
def service_unavailable(e):
    REQUEST_ERRORS.inc()
    return Response('{"error": "Service Unavailable"}', status=503, content_type="application/json")

# Prometheus metrics endpoint
@app.route('/metrics', methods=['GET'])
def metrics():
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)

if __name__ == '__main__':
    logger.add("api.log", rotation="10 MB", retention="7 days")
    app.run(host='0.0.0.0', port=8082, debug=False)
