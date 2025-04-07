"""
THIS FILE CONTAINS THE CLASS DBMANAGER THAT MANAGES CONNECTION FROM PSQL,
ALSO CONTAINS METHODS TO INSERT VALUES INTO THE DB.
ALSO CONTAINS METHODS TO INSERT VALUES INTO THE DB FROM A CSV FILE.
"""

import psycopg2
from psycopg2 import sql
from typing import Dict, List, Any, Tuple, Optional
import logging

class DBManager:
    """
    A class for managing operations on a PostgreSQL database.
    Handles connection management and data insertion operations.
    """
    
    def __init__(self, host: str = 'localhost', port: int = 5432,
                 user: str = 'user1', password: str = 'chicago', 
                 database: str = 'netflicksdb'):
        """
        Initialize the database manager with connection parameters.
        
        Args:
            host: Database server hostname
            port: Database server port
            user: Database username
            password: Database password
            database: Database name
        """
        self.connection_params = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': database
        }
        self.conn = None
        self.cursor = None
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        """Set up and configure logger for the DBManager to log errors only."""
        logger = logging.getLogger('DBManager')
        logger.setLevel(logging.ERROR)  # Changed from INFO to ERROR
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def connect(self) -> bool:
        """
        Establish a connection to the database.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.conn = psycopg2.connect(**self.connection_params)
            self.cursor = self.conn.cursor()
            return True
        except Exception as e:
            self.logger.error(f"Connection error: {str(e)}")
            return False
    
    def disconnect(self) -> None:
        """Close the database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        
        self.cursor = None
        self.conn = None
    
    def __enter__(self):
        """Context manager entry point."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point."""
        self.disconnect()
        
    def insert_record(self, table: str, data: Dict[str, Any]) -> bool:
        """
        Insert a single record into the specified table.
        
        Args:
            table: Table name
            data: Dictionary with column names as keys and values to insert
            
        Returns:
            bool: True if insertion successful, False otherwise
        """
        if not self.conn or self.conn.closed:
            self.connect()
            
        columns = list(data.keys())
        values = list(data.values())
        
        # Create placeholders for values (%s, %s, ...)
        placeholders = ', '.join(['%s'] * len(values))
        
        query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(placeholders)
        )
        
        try:
            self.cursor.execute(query, values)
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error inserting into {table}: {str(e)}")
            return False
    
    def insert_many(self, table: str, columns: List[str], values: List[Tuple]) -> int:
        """
        Insert multiple records into the specified table.
        
        Args:
            table: Table name
            columns: List of column names
            values: List of tuples containing values to insert
            
        Returns:
            int: Number of records inserted successfully
        """
        if not self.conn or self.conn.closed:
            self.connect()
            
        # Create placeholders for values (%s, %s, ...)
        placeholders = ', '.join(['%s'] * len(columns))
        
        query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(placeholders)
        )
        
        try:
            self.cursor.executemany(query, values)
            self.conn.commit()
            return self.cursor.rowcount
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error batch inserting into {table}: {str(e)}")
            return 0
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Tuple]:
        """
        Execute a custom query.
        
        Args:
            query: SQL query string
            params: Parameters for the query (optional)
            
        Returns:
            List of tuples containing the query results
        """
        if not self.conn or self.conn.closed:
            self.connect()
            
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
                
            if query.strip().upper().startswith(('INSERT', 'UPDATE', 'DELETE')):
                self.conn.commit()
                return []
            else:
                return self.cursor.fetchall()
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Query execution error: {str(e)}")
            return []
        
    def insert_from_csv(self, csv_filename: str, table_name: str):
        
        rows_inserted = self.load_csv_to_table(
            csv_file_path=csv_filename,
            table_name=table_name,
            delimiter=',',
            has_header=True,
            batch_size=100000  # Adjust based on your system's memory
        )
        print(f"Inserted {rows_inserted} rows into movies table")