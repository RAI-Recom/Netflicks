"""
THIS FILE CONTAINS THE CLASS DBMANAGER THAT MANAGES CONNECTION FROM PSQL,
ALSO CONTAINS METHODS TO INSERT VALUES INTO THE DB.
ALSO CONTAINS METHODS TO INSERT VALUES INTO THE DB FROM A CSV FILE.
"""

import psycopg2
from psycopg2 import sql
from typing import Dict, List, Any, Tuple, Optional
import logging
import os
from dotenv import load_dotenv

class DBManager:
    """
    A class for managing operations on a PostgreSQL database.
    Handles connection management and data insertion operations.
    """
    
    def __init__(self, host: str = 'localhost', port: int = 5432, schema: str = 'public'):
        """
        Initialize the database manager with connection parameters.
        
        Args:
            host: Database server hostname
            port: Database server port
            schema: Database schema name (default is 'public')
        """
        # Load environment variables from .env file
        load_dotenv()
        self.connection_params = {
            'host': host,
            'port': port,
            'user': os.getenv('DB_USER'),  # Fetch username from .env
            'password': os.getenv('DB_PASSWORD'),  # Fetch password from .env
            'database': os.getenv('DB_NAME'),  # Corrected to fetch the database name
            'options': f'-c search_path={schema}'  # Set the schema
        }
        self.conn = None
        self.cursor = None
        self.logger = self._setup_logger()
        
        # Connect to the database and create tables if they do not exist
        if self.connect():
            self.create_tables()
        
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
        
    def create_tables(self) -> None:
        """Create necessary tables in the database."""
        create_movies_table = """
        CREATE TABLE IF NOT EXISTS movies (
            movie_id SERIAL PRIMARY KEY,
            movie_title_id TEXT NOT NULL,
            title TEXT,
            year INTEGER,
            rating NUMERIC,
            genres TEXT[],  -- To store an array of genres
            plot TEXT,
            duration INTEGER,  -- Renamed from runtime
            directors TEXT,  -- To store the directors
            actors TEXT,  -- To store the actors
            votes INTEGER,  -- To store the number of votes
            languages TEXT[],  -- To store an array of languages
            country TEXT[],  -- To store an array of countries
            release_date DATE,  -- To store the release date
            poster TEXT,  -- To store the poster URL
            UNIQUE (title, movie_title_id)
        );
        """

        create_ratings_table = """
        CREATE TABLE IF NOT EXISTS ratings (
            rating_id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            movie_id INTEGER NOT NULL,
            rating INTEGER CHECK (rating >= 1 AND rating <= 5),
            updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
            FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
        );
        """

        create_users_table = """
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT NOT NULL PRIMARY KEY
        );
        """

        create_watch_history_table = """
        CREATE TABLE IF NOT EXISTS watch_history (
            watch_id SERIAL PRIMARY KEY,
            updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
            user_id BIGINT NOT NULL,
            movie_id INTEGER NOT NULL,
            watched_minutes INTEGER,
            FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
        );
        """

        try:
            self.cursor.execute(create_movies_table)
            self.cursor.execute(create_ratings_table)
            self.cursor.execute(create_users_table)
            self.cursor.execute(create_watch_history_table)
            self.conn.commit()
            print("Tables created successfully.")
            
            # Create the function and trigger
            self.create_function_and_trigger()
            
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error creating tables: {str(e)}")
    
    def create_function_and_trigger(self) -> None:
        """Create function and trigger to ensure user and movie exist."""
        create_function = """
        CREATE OR REPLACE FUNCTION ensure_user_and_movie_exist() 
        RETURNS TRIGGER AS $$
        DECLARE 
            default_title_id INTEGER := 1; -- Set a valid default if needed
        BEGIN
            -- Ensure user exists
            IF NOT EXISTS (SELECT 1 FROM users WHERE user_id = NEW.user_id) THEN
                INSERT INTO users(user_id) VALUES (NEW.user_id);
            END IF;

            -- Ensure movie exists with movie_title_id
            IF NOT EXISTS (SELECT 1 FROM movies WHERE movie_id = NEW.movie_id) THEN
                INSERT INTO movies(movie_id, movie_title_id) VALUES (NEW.movie_id, default_title_id);
            END IF;

            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """

        create_trigger = """
        CREATE TRIGGER trigger_ensure_user_movie
        BEFORE INSERT ON ratings
        FOR EACH ROW
        EXECUTE FUNCTION ensure_user_and_movie_exist();

        CREATE TRIGGER trigger_ensure_user_movie_watch_history
        BEFORE INSERT ON watch_history
        FOR EACH ROW
        EXECUTE FUNCTION ensure_user_and_movie_exist();
        """

        try:
            self.cursor.execute(create_function)
            self.cursor.execute(create_trigger)
            self.conn.commit()
            print("Function and trigger created successfully.")
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error creating function or trigger: {str(e)}")
    
    def ensure_movie_exists(self, movie_title_id: str) -> int:
        """
        Ensure that a movie with the given movie_title_id exists in the movies table.
        If it does not exist, insert a new record and return the movie_id.
        
        Args:
            movie_title_id: The movie title ID to check for existence.
            
        Returns:
            int: The movie_id of the existing or newly inserted movie.
        """
        # Check if the movie exists
        query = "SELECT movie_id FROM movies WHERE movie_title_id = %s"
        self.cursor.execute(query, (movie_title_id,))
        result = self.cursor.fetchone()
        
        if result:
            return result[0]  # Return existing movie_id
        
        # If movie does not exist, insert a new record
        insert_query = "INSERT INTO movies (movie_title_id) VALUES (%s) RETURNING movie_id"
        self.cursor.execute(insert_query, (movie_title_id,))
        new_movie_id = self.cursor.fetchone()[0]
        self.conn.commit()
        
        return new_movie_id  # Return the new movie_id
    
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
        
        # Ensure the movie exists if the table is ratings or watch_history
        if table in ['ratings', 'watch_history'] and 'movie_title_id' in data:
            movie_id = self.ensure_movie_exists(data['movie_title_id'])
            data['movie_id'] = movie_id  # Update the data dictionary with the movie_id
        
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
        
        # Create a copy of the columns list to avoid modifying the original
        columns_copy = columns.copy()
        
        # Ensure the movie exists if the table is ratings or watch_history
        if table in ['ratings', 'watch_history']:
            try:
                # Find the index of movie_title_id in the columns list
                movie_title_id_index = columns_copy.index('movie_title_id')
                
                # Create a new list to store the updated values
                updated_values = []
                
                for value in values:
                    # Convert tuple to list for modification
                    value_list = list(value)
                    # Get the movie_title_id from the values list using the found index
                    movie_title_id = value_list[movie_title_id_index]
                    movie_id = self.ensure_movie_exists(movie_title_id)
                    # Update the movie_id in the values list
                    value_list[movie_title_id_index] = movie_id
                    # Convert back to tuple and add to updated values
                    updated_values.append(tuple(value_list))
                
                # Update the values list with the modified values
                values = updated_values
                
                # Update the column name from movie_title_id to movie_id
                columns_copy[movie_title_id_index] = 'movie_id'
                
            except ValueError:
                # Handle the case where 'movie_title_id' is not in the columns list
                self.logger.error("'movie_title_id' not found in columns list")
                return 0
        
        # Create placeholders for values (%s, %s, ...)
        placeholders = ', '.join(['%s'] * len(columns_copy))
        
        query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table),
            sql.SQL(', ').join(map(sql.Identifier, columns_copy)),
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