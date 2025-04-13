import sys
sys.path.append('.')

from flask import Flask, jsonify, Response
from loguru import logger
# from model_training.model_service import load_popularity_model, load_user_recommendations_model
# from pipeline_testing.hybrid_recommend import hybrid_recommend
app = Flask(__name__)

# popularity_model = load_popularity_model()
# user_recommendations = load_user_recommendations_model()

@app.route('/recommend/<int:user_id>', methods=['GET'])
def recommend_movies(user_id):
    try:
        # if user_id in user_recommendations:
        #     logger.info("Recommendation from user_recommendations.")

        #     return jsonify(user_recommendations[user_id])
         
        return jsonify(hybrid_recommend(user_id, 20))

        logger.info("Recommendation from popularity_model.")
        movie_id_op = [movie[0] for movie in popularity_model[:20]]  # Extract only movie titles
        return jsonify(movie_id_op)
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
    app.run(host='0.0.0.0', port=8082, debug=True)  