"""
THIS FILE LISTENS TO KAFKA LIVE STREAM, PROCESSES THE DATA USING DATAFORMATTER, 
AND STORES IT IN THE DATABASE.
"""


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

    def start_consuming(self, write_to_file: bool = False, batch_size: int = 100):
        """
        Start consuming messages from Kafka topic.
        
        Args:
            write_to_file: Whether to write messages to output file
            batch_size: Number of messages to process in each batch
        """
        if not self.consumer:
            if not self.connect():
                return
            
        self.logger.info('Started reading from Kafka broker')
        try:
            messages = []
            for message in self.consumer:
                decoded_message = message.value.decode()
                messages.append(decoded_message)
                
                if write_to_file:
                    self.write_to_file(decoded_message)
                
                # Process batch when it reaches batch_size
                if len(messages) >= batch_size:
                    self.process_and_store_data(messages)
                    messages = []  # Clear the batch
                    
        except Exception as e:
            self.logger.error(f"Error while consuming messages: {str(e)}")
        finally:
            # Process any remaining messages
            if messages:
                self.process_and_store_data(messages)
            self.close()
    
    def close(self):
        """Close the Kafka consumer connection."""
        if self.consumer:
            self.consumer.close()
            self.logger.info("Kafka consumer connection closed")

# Example usage
if __name__ == "__main__":
    reader = KafkaStreamReader()
    # Process in batches of 100 messages
    reader.start_consuming(batch_size=100)
