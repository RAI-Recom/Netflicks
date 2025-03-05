from flask import Flask, jsonify, Response
import mock

app = Flask(__name__)

@app.route('/recommend/<int:user_id>', methods=['GET'])
def recommend_movies(user_id):
    try:
        movies = mock.MOVIE_RECOMMENDATIONS.get(user_id)
        print(type(movies))
        if not movies:
            return Response(status=204) # no movies

        movie_list = ",".join(str(movies["id"]) for movies in movies)
        return Response(movie_list, status=200, content_type="text/plain")

    except Exception as e:
        return Response('{"error": "Internal Server Error   "}', status=500, content_type="application/json")

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
    app.run(host='0.0.0.0', port=5000, debug=True)