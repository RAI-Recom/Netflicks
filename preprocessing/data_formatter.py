"""
THIS FILE CONTAINS THE CLASS DATAFORMATTING THAT CONTAINS process_chunk()
FUNCTION THAT PRE PROCESSES THE DATA FROM THE KAFKA STREAM INTO DATA AND RATE ENTRIES.
"""


import pandas as pd
import logging
from typing import Tuple, Dict, List, Any

class DataFormatter:
    """
    A class for formatting and transforming log data before database insertion.
    Handles parsing and transforming raw log data into structured formats.
    """
    
    def __init__(self):
        """Initialize the data formatting class with an error-only logger."""
        self.logger = self._setup_logger()
    
    def _setup_logger(self) -> logging.Logger:
        """Set up and configure error-only logger for the DataFormatter class."""
        logger = logging.getLogger('DataFormatter')
        logger.setLevel(logging.ERROR)  # Only capture errors and above
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def process_chunk(self, chunk: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Process a chunk of data and return transformed DataFrames for Data and Rate entries.
        """
        data_entries = []
        rate_entries = []
        
        for _, row in chunk.iterrows():
            try:
                req = row["request"]
                
                if req.startswith("GET /data/"):
                    parts = req.split("/data/")[1].split("/")
                    if len(parts) < 3:
                        continue
                    
                    movie_id = parts[1].replace("+", " ")
                    minutes = parts[2].split(".")[0]
                    
                    data_entries.append({
                        "timestamp": row["timestamp"],
                        "user_id": row["request_id"],
                        "movie_title": movie_id,
                        "watched_minutes": minutes
                    })
                    
                elif req.startswith("GET /rate/"):
                    rate_part = req.split("/rate/")[1]
                    movie_rating = rate_part.split("=")
                    if len(movie_rating) != 2:
                        continue
                    
                    movie_id = movie_rating[0].replace("+", " ")
                    
                    rate_entries.append({
                        "timestamp": row["timestamp"],
                        "user_id": row["request_id"],
                        "movie_title": movie_id,
                        "rating": movie_rating[1]
                    })
                    
            except Exception as e:
                self.logger.error(f"Error processing row: {e}")
                continue
        
        return pd.DataFrame(data_entries), pd.DataFrame(rate_entries)
    
    def process_file(self, file_path: str, chunk_size: int = 10000) -> Dict[str, pd.DataFrame]:
        """
        Process a large log file in chunks and return DataFrames for data and rate entries.
        """
        # Initialize empty DataFrames for results
        all_data_entries = pd.DataFrame()
        all_rate_entries = pd.DataFrame()
        
        # Process file in chunks
        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            data_df, rate_df = self.process_chunk(chunk)
            
            # Append results
            all_data_entries = pd.concat([all_data_entries, data_df], ignore_index=True)
            all_rate_entries = pd.concat([all_rate_entries, rate_df], ignore_index=True)
        
        return {
            'data': all_data_entries,
            'rate': all_rate_entries
        }
    
    def save_processed_data(self, processed_data: Dict[str, pd.DataFrame], output_dir: str) -> Dict[str, str]:
        """
        Save processed DataFrames to CSV files.
        """
        import os
        
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        file_paths = {}
        
        for data_type, df in processed_data.items():
            if not df.empty:
                file_path = os.path.join(output_dir, f"{data_type}_entries.csv")
                df.to_csv(file_path, index=False)
                file_paths[data_type] = file_path
                
        return file_paths
