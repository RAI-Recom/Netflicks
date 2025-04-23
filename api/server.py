import sys
sys.path.append('.')

from flask import Flask, jsonify, Response
from loguru import logger
from pipeline.hybrid_recommend import hybrid_recommend
import os
from dotenv import load_dotenv
load_dotenv()

app = Flask(__name__)


@app.route('/recommend/<int:user_id>', methods=['GET'])
def recommend_movies(user_id):
    try: 
        return jsonify(hybrid_recommend(user_id, 20))

    except Exception as e:
        logger.error(e)
        return Response('{"error": "Internal Server Error"}', status=500, content_type="application/json")   

# user id does not exist 
@app.errorhandler(400)
def bad_request(e):
    return Response('{"error": "Invalid request. Please check the user ID."}', status=400, content_type="application/json")

# too many requests
@app.errorhandler(429)
def too_many_requests(e):
    return Response('{"error": "Too Many Requests"}', status=429, content_type="application/json")

# if the service is unavailable or the ml model is down
@app.errorhandler(503)
def service_unavailable(e):
    return Response('{"error": "Service Unavailable"}', status=503, content_type="application/json")

if __name__ == '__main__':
    logger.add("api.log")
    app.run(host='0.0.0.0', port=os.getenv('API_PORT'), debug=True)  