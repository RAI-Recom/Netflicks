from evaluation.online_evaluator import OnlineEvaluator

import os
from kafka import KafkaConsumer
import logging
from preprocessing.data_formatter import DataFormatting
from db.db_manager import DBManager
import pandas as pd
from typing import Dict, List

class KafkaStreamReader:
    """
    A class to handle Kafka stream reading operations.
    
    Processes three types of messages from the Kafka stream:
    1. Recommendation requests:
       <time>,<userid>,recommendation request <server>, status <200 for success>, 
       result: <recommendations>, <responsetime>
       - Indicates when a user requests movie recommendations
       - Includes the recommendations provided or error message
    
    2. Movie watching events:
       <time>,<userid>,GET /data/m/<movieid>/<minute>.mpg
       - Indicates a user watching a movie
       - Movies are split into 1-minute mpg files
       - Each request represents one minute of viewing time
    
    3. Movie rating events:
       <time>,<userid>,GET /rate/<movieid>=<rating>
       - User rating a movie
       - Ratings range from 1 to 5 stars
    """
    
    def __init__(self, topic: str = 'movielog1', 
                 bootstrap_servers: list = ['localhost:9092'],
                 group_id: str = 'team101',
                 output_file: str = '../data/movie_logs.csv'):
        """
        Initialize the Kafka stream reader.
        
        Args:
            topic: Kafka topic to consume
            bootstrap_servers: List of Kafka broker addresses
            group_id: Consumer group ID
            output_file: Path to output CSV file
        """
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.output_file = output_file
        self.consumer = None
        self.logger = self._setup_logger()
        self.data_formatter = DataFormatting()
        self.db_manager = DBManager()
        self.online_evaluator = OnlineEvaluator()

        
    def _setup_logger(self) -> logging.Logger:
        """Set up and configure logger."""
        logger = logging.getLogger('KafkaStreamReader')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def connect(self) -> bool:
        """
        Establish connection to Kafka broker.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='latest',
                enable_auto_commit=False,
                group_id=self.group_id,
                auto_commit_interval_ms=1000
            )
            
            # Verify connection by checking partitions
            partitions = self.consumer.partitions_for_topic(self.topic)
            self.logger.info(f"Connected successfully. Available partitions: {partitions}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Kafka broker: {str(e)}")
            return False
    
    def write_to_file(self, message: str) -> bool:
        """
        Write message to output file.
        
        Args:
            message: Message to write
            
        Returns:
            bool: True if write successful, False otherwise
        """
        try:
            with open(self.output_file, 'a') as f:
                f.write(f"{message}\n")
            return True
        except Exception as e:
            self.logger.error(f"Failed to write to file: {str(e)}")
            return False
    
    def process_and_store_data(self, messages: List[str]) -> Dict[str, int]:
        """
        Process messages using DataFormatting and store in PostgreSQL.
        
        Args:
            messages: List of raw message strings
            
        Returns:
            Dict containing count of inserted data and rate entries
        """
        try:
            # Create DataFrame from messages
            df = pd.DataFrame({
                'request': messages,
                'timestamp': pd.Timestamp.now(),  # You might want to extract actual timestamps
                'request_id': range(len(messages))  # You might want to extract actual request IDs
            })
            
            # Process data using DataFormatting
            data_df, rate_df = self.data_formatter.process_chunk(df)
            
            inserted_counts = {'data': 0, 'rate': 0}
            
            # Insert data entries
            if not data_df.empty:
                data_values = [tuple(x) for x in data_df.values]
                data_columns = list(data_df.columns)
                inserted_counts['data'] = self.db_manager.insert_many(
                    'data_entries',
                    data_columns,
                    data_values
                )
                
            # Insert rate entries
            if not rate_df.empty:
                rate_values = [tuple(x) for x in rate_df.values]
                rate_columns = list(rate_df.columns)
                inserted_counts['rate'] = self.db_manager.insert_many(
                    'rate_entries',
                    rate_columns,
                    rate_values
                )
                
            self.logger.info(f"Inserted {inserted_counts['data']} data entries and {inserted_counts['rate']} rate entries")
            return inserted_counts
            
        except Exception as e:
            self.logger.error(f"Error in process_and_store_data: {str(e)}")
            return {'data': 0, 'rate': 0}

    def start_consuming(self, write_to_file: bool = False, batch_size: int = 100, max_messages: int = 10000):
        """
        Start consuming messages from Kafka topic.

        Args:
            write_to_file: Whether to write messages to output file
            batch_size: Number of messages to process in each batch (optional, deprecated here)
            max_messages: Maximum number of messages to consume before stopping
        """
        if not self.consumer:
            if not self.connect():
                return

        self.logger.info('Started reading from Kafka broker')
        message_count = 0

        try:
            for message in self.consumer:
                decoded_message = message.value.decode()

                if "recommendation request" in decoded_message:
                    user_id, recommended_movies = self.extract_recommendation(decoded_message)
                    if user_id is not None:
                        self.online_evaluator.process_recommendation(user_id, recommended_movies)

                elif "/data/m/" in decoded_message:
                    user_id, movie_id = self.extract_watch(decoded_message)
                    if user_id is not None and movie_id is not None:
                        self.online_evaluator.process_watch(user_id, movie_id)

                elif "/rate/" in decoded_message:
                    user_id, movie_id, rating = self.extract_rating(decoded_message)
                    if user_id is not None and movie_id is not None and rating is not None:
                        self.online_evaluator.process_rating(user_id, movie_id, rating)

                # Update message count
                message_count += 1

                # Print metrics every 500 messages
                if message_count % 500 == 0:
                    hitrate, rmse = self.online_evaluator.compute_metrics()
                    print(f"Live HitRate@{self.online_evaluator.k}: {hitrate:.4f}, Live RMSE: {rmse:.4f}" if rmse is not None else "No RMSE yet")

                # Stop if maximum number of messages consumed
                if message_count >= max_messages:
                    self.logger.info(f"Reached maximum message limit ({max_messages}). Stopping consumption.")
                    break

        except Exception as e:
            self.logger.error(f"Error while consuming messages: {str(e)}")

        finally:
            self.logger.info("Closing Kafka consumer connection.")
            self.close()

    def close(self):
        """Close the Kafka consumer connection."""
        if self.consumer:
            self.consumer.close()
            self.logger.info("Kafka consumer connection closed")

    def extract_recommendation(self, message: str):
        """
        Extract user_id and recommended movie_ids from a recommendation message.
        """
        try:
            parts = message.split(',')
            user_id = int(parts[1])

            # Find the part with "result:" and extract movie IDs
            result_part = [p for p in parts if "result:" in p]
            if result_part:
                movies_str = result_part[0].split("result:")[-1].strip()
                movie_ids = [int(m.strip()) for m in movies_str.strip('[]').split(',') if m.strip().isdigit()]
            else:
                movie_ids = []

            return user_id, movie_ids
        except Exception as e:
            self.logger.error(f"Failed to parse recommendation message: {str(e)}")
            return None, []

    def extract_watch(self, message: str):
        """
        Extract user_id and movie_id from a watch message.
        """
        try:
            parts = message.split(',')
            user_id = int(parts[1])
            
            # Example: /data/m/1234/56.mpg → movie_id = 1234
            movie_part = [p for p in parts if "/data/m/" in p]
            if movie_part:
                path_parts = movie_part[0].split('/')
                movie_id = int(path_parts[3])
            else:
                movie_id = None
            
            return user_id, movie_id
        except Exception as e:
            self.logger.error(f"Failed to parse watch message: {str(e)}")
            return None, None

    def extract_rating(self, message: str):
        """
        Extract user_id, movie_id, and rating from a rating message.
        """
        try:
            parts = message.split(',')
            user_id = int(parts[1])
            
            # Example: /rate/1234=5 → movie_id = 1234, rating = 5
            rating_part = [p for p in parts if "/rate/" in p]
            if rating_part:
                rating_info = rating_part[0].split('/rate/')[-1]
                movie_id, rating = map(float, rating_info.split('='))
                movie_id = int(movie_id)
            else:
                movie_id, rating = None, None
            
            return user_id, movie_id, rating
        except Exception as e:
            self.logger.error(f"Failed to parse rating message: {str(e)}")
            return None, None, None


# Example usage
if __name__ == "__main__":
    reader = KafkaStreamReader()
    # Process in batches of 100 messages
    reader.start_consuming(batch_size=100)