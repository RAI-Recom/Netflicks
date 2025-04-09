"""
THIS FILE CONTAINS THE CLASS DATAFORMATTING THAT CONTAINS process_chunk()
FUNCTION THAT PRE PROCESSES THE DATA FROM THE KAFKA STREAM INTO DATA AND RATE ENTRIES.
"""


import pandas as pd
import logging
from typing import Tuple, Dict, List, Any
from datetime import datetime

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
    
    def process_chunk(self, messages: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Process a chunk of data and return transformed DataFrames for Data and Rate entries.
        """
        data_entries = []
        rate_entries = []
        
        for message in messages:
            try:
                data = message.split(",")

                if data[2].startswith("GET /data/"):
                    value = {
                            "updated_at": datetime.fromisoformat(data[0]),
                            "user_id": data[1],
                            "movie_title_id": data[2].split("/data/")[1].split("/")[1],
                            "watched_minutes": int(data[2].split("/data/")[1].split("/")[2].split(".")[0])
                        } 
                    
                    data_entries.append(value)
                    
                elif data[2].startswith("GET /rate/"):
                    value = {
                            "updated_at": datetime.fromisoformat(data[0]),
                            "user_id": data[1],
                            "movie_title_id": data[2].split("/rate/")[1].split("=")[0],
                            "rating": int(data[2].split("/rate/")[1].split("=")[1])
                        } 
                    rate_entries.append(value)
                    
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
