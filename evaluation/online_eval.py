import sys
sys.path.append('.')

import os
import logging
import pandas as pd
import numpy as np
from kafka import KafkaConsumer
from preprocessing.data_formatter import DataFormatter
from pipeline.hybrid_recommend import hybrid_recommend, cf_model, movie_id_to_title, user_profiles
from sklearn.metrics import mean_squared_error
from tqdm import tqdm

class KafkaStreamReader:
    def __init__(self, topic: str = 'movielog1', 
                 bootstrap_servers: list = ['localhost:9092'],
                 group_id: str = 'team101'):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.consumer = None
        self.logger = self._setup_logger()
        self.data_formatter = DataFormatter()
        self.rate_records = []  # Store all rating records
        self.K = 20
        self.DEFAULT_PRED_RATING = 3.0

    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger('KafkaStreamReader')
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger

    def connect(self) -> bool:
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='latest',
                enable_auto_commit=False,
                group_id=self.group_id,
                auto_commit_interval_ms=1000
            )
            partitions = self.consumer.partitions_for_topic(self.topic)
            self.logger.info(f"Connected successfully. Available partitions: {partitions}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to Kafka broker: {str(e)}")
            return False

    def start_consuming(self, batch_size: int = 100, max_messages: int = 10000):
        if not self.consumer:
            if not self.connect():
                return

        self.logger.info('Started reading from Kafka broker')
        messages = []
        message_count = 0

        try:
            for message in self.consumer:
                decoded_message = message.value.decode()
                messages.append(decoded_message)
                message_count += 1

                if len(messages) >= batch_size:
                    self.process_batch(messages)
                    messages = []

                if message_count >= max_messages:
                    self.logger.info(f"Reached maximum message limit ({max_messages}). Stopping consumption.")
                    break

        except Exception as e:
            self.logger.error(f"Error while consuming messages: {str(e)}")
        finally:
            if messages:
                self.process_batch(messages)
            self.evaluate()
            self.close()

    def process_batch(self, messages):
        """
        Process a batch of messages to extract ratings.
        """
        try:
            _, rate_df = self.data_formatter.process_chunk(messages)
            if not rate_df.empty:
                self.rate_records.append(rate_df)
        except Exception as e:
            self.logger.error(f"Error processing batch: {str(e)}")

    def evaluate(self):
        """
        Compute HitRate and RMSE based on collected ratings.
        """
        if not self.rate_records:
            print("No ratings collected. Cannot evaluate.")
            return

        ratings_df = pd.concat(self.rate_records, ignore_index=True)
        ratings_df = ratings_df.dropna(subset=["rating"])

        actual_ratings = []
        predicted_ratings = []
        hits = 0
        total_users = 0

        user_groups = ratings_df.groupby('user_id')

        for user_id, group in tqdm(user_groups, desc="Evaluating"):
            true_movies = group['movie_id'].tolist()
            true_ratings = group['rating'].tolist()

            recommended_titles = hybrid_recommend(user_id, top_n=self.K)
            true_movie_titles = [movie_id_to_title.get(mid, f"Unknown Title ({mid})") for mid in true_movies]

            user_hit = any(title in recommended_titles for title in true_movie_titles)
            if user_hit:
                hits += 1
            total_users += 1

            for movie_id, true_rating in zip(true_movies, true_ratings):
                try:
                    pred = cf_model.predict(str(user_id), str(movie_id)).est
                except Exception:
                    pred = self.DEFAULT_PRED_RATING
                actual_ratings.append(true_rating)
                predicted_ratings.append(pred)

        if actual_ratings:
            rmse = np.sqrt(mean_squared_error(actual_ratings, predicted_ratings))
        else:
            rmse = None
        hitrate = hits / total_users if total_users > 0 else 0.0

        print("\n=== Final Online Evaluation Metrics ===")
        print(f"HitRate@{self.K}: {hitrate:.4f}")
        print(f"RMSE: {rmse:.4f}" if rmse is not None else "RMSE: Not available")

    def close(self):
        if self.consumer:
            self.consumer.close()
            self.logger.info("Kafka consumer connection closed")

if __name__ == "__main__":
    reader = KafkaStreamReader()
    reader.start_consuming(batch_size=100, max_messages=10000)
