FROM python:3.10.12

WORKDIR /app
COPY apis/requirements.txt .
COPY apis/server.py .
COPY models/popular_movies.pkl models/
COPY models/user_recommendations.pkl models/

RUN pip install --no-cache-dir -r requirements.txt

ENV FLASK_APP=server.py
EXPOSE 8082

CMD ["flask", "run", "--host", "0.0.0.0", "--port", "8082"]
