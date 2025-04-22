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
import ast
import csv
from datetime import datetime
import pandas as pd

class DBManager:
    """
    A class for managing operations on a PostgreSQL database.
    Handles connection management and data insertion operations.
    """
    
    def __init__(self, host: str = 'localhost', schema: str = 'public'):
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
            'port': os.getenv('DB_PORT'),
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
            imdb_id TEXT,
            movie_title_id TEXT NOT NULL UNIQUE,
            title TEXT,
            year INTEGER,
            rating NUMERIC,
            genres TEXT[],  -- To store an array of genres
            plot TEXT,
            duration INTEGER,  -- Renamed from runtime
            directors TEXT[],  -- To store the directors
            actors TEXT[],  -- To store the actors
            votes INTEGER,  -- To store the number of votes
            languages TEXT[],  -- To store an array of languages
            country TEXT[],  -- To store an array of countries
            release_date DATE,  -- To store the release date
            poster TEXT,  -- To store the poster URL
            UNIQUE (title, movie_title_id)
        );
        """

        create_users_table = """
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT NOT NULL PRIMARY KEY
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
            self.cursor.execute(create_users_table)
            self.cursor.execute(create_ratings_table)
            self.cursor.execute(create_watch_history_table)
            self.conn.commit()
            print("Tables created/checked successfully.")
            
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
        CREATE OR REPLACE TRIGGER trigger_ensure_user_movie
        BEFORE INSERT ON ratings
        FOR EACH ROW
        EXECUTE FUNCTION ensure_user_and_movie_exist();

        CREATE OR REPLACE TRIGGER trigger_ensure_user_movie_watch_history
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
            del data['movie_title_id']  # Remove movie_title_id from the data dictionary

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
        
    # def insert_from_csv(self, csv_filename: str, table_name: str):
        
    #     rows_inserted = self.load_csv_to_table(
    #         csv_file_path=csv_filename,
    #         table_name=table_name,
    #         delimiter=',',
    #         has_header=True,
    #         batch_size=100000  # Adjust based on your system's memory
    #     )
    #     print(f"Inserted {rows_inserted} rows into movies table")
        
    def load_movies_from_csv(self, csv_filename):
        """
        Load movie data from movies_metadata.csv into the movies table.
        
        Args:
            csv_filename: Path to the movies_metadata.csv file
            
        Returns:
            int: Number of rows inserted
        """
        
        # Define mapping between CSV headers and table columns
        column_mapping = {
            'movie_id': 'imdb_id',
            'movie_title_id': 'movie_title_id',
            'title': 'title',
            'year': 'year',
            'rating': 'rating',
            'genres': 'genres',
            'plot': 'plot',
            'runtime': 'runtime',
            'directors': 'directors',
            'cast': 'actors',  # Note: CSV has 'cast' but table has 'actors'
            'votes': 'votes',
            'languages': 'languages',
            'country': 'country',
            'release_date': 'release_date',
            'poster': 'poster'
        }
        
        if not self.conn or self.conn.closed:
            self.connect()
        
        # Get all columns from the table
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'movies'
        """)
        table_columns_with_types = {col[0]: col[1] for col in cursor.fetchall()}
        table_columns = list(table_columns_with_types.keys())
        
        rows_inserted = 0
        skipped_rows = 0
        batch_size = 10000  # Adjust based on your system's memory
        batch = []
        
        with open(csv_filename, 'r', encoding='utf-8') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            
            # Extract all the columns we need to insert into
            db_columns = []
            for db_col in table_columns:
                if db_col in [column_mapping.get(csv_col) for csv_col in column_mapping]:
                    db_columns.append(db_col)
            
            # Create placeholders for the SQL query
            placeholders = ', '.join(['%s' for _ in db_columns])
            
            # Construct SQL statement
            insert_stmt = sql.SQL("INSERT INTO movies ({}) VALUES ({}) ON CONFLICT (movie_title_id) DO NOTHING").format(
                sql.SQL(', ').join(map(sql.Identifier, db_columns)),
                sql.SQL(placeholders)
            )
            
            # Process each row in the CSV
            for row_num, row in enumerate(csv_reader, start=1):
                try:
                    # Prepare values for this row
                    values = []
                    
                    # For each database column, get the value from CSV or default
                    for db_col in db_columns:
                        # Find which CSV column maps to this DB column
                        csv_cols = [csv_col for csv_col, mapped_col in column_mapping.items() if mapped_col == db_col]
                        
                        if csv_cols and csv_cols[0] in row:
                            # Get value from CSV
                            csv_col = csv_cols[0]
                            value = row[csv_col]
                            
                            # Check for N/A values first
                            if value is None or value.strip() == '' or value.upper() == 'N/A':
                                value = None
                            else:
                                # Data type conversions
                                if db_col == 'year':
                                    try:
                                        value = int(value)
                                    except ValueError:
                                        value = None
                                elif db_col == 'rating':
                                    try:
                                        value = float(value)
                                    except ValueError:
                                        value = None
                                elif db_col == 'genres':
                                    # Convert genres string to list of strings
                                    # Split by comma and remove whitespace
                                    genres_list = [genre.strip() for genre in value.split(',')]
                                    # For PostgreSQL ARRAY type
                                    value = genres_list
                                elif db_col == 'directors':
                                    # Convert directors string to list of strings
                                    # Split by comma and remove whitespace
                                    directors_list = [director.strip() for director in value.split(',')]
                                    # For PostgreSQL ARRAY type
                                    value = directors_list
                                elif db_col == 'actors':
                                    # Convert cast string to list of strings
                                    # Split by comma and remove whitespace
                                    actors_list = [actor.strip() for actor in value.split(',')]
                                    # For PostgreSQL ARRAY type
                                    value = actors_list
                                elif db_col == 'country':
                                    # Handle country which may be in string list format ['Canada', 'United Kingdom']
                                    try:
                                        if value.startswith('[') and value.endswith(']'):
                                            # Parse the string as a Python literal list
                                            country_list = ast.literal_eval(value)
                                            value = country_list
                                        else:
                                            # If it's a single country as string
                                            value = [value.strip()]
                                    except (ValueError, SyntaxError):
                                        value = []
                                elif db_col == 'languages':
                                    # Handle languages which may also be in list format
                                    try:
                                        if value.startswith('[') and value.endswith(']'):
                                            # Parse the string as a Python literal list
                                            languages_list = ast.literal_eval(value)
                                            value = languages_list
                                        else:
                                            # If it's a single language as string
                                            value = [value.strip()]
                                    except (ValueError, SyntaxError):
                                        value = []
                                elif db_col == 'runtime':
                                    # Convert runtime from list of strings to integer
                                    try:
                                        # The input format appears to be a string representation of a list: ['96']
                                        # First, check if it looks like a list
                                        if value.startswith('[') and value.endswith(']'):
                                            # Try to parse with ast.literal_eval for safety
                                            try:
                                                runtime_list = ast.literal_eval(value)
                                                # Take the first item if it's a list
                                                if isinstance(runtime_list, list) and len(runtime_list) > 0:
                                                    runtime_str = str(runtime_list[0])
                                                    value = int(runtime_str) if runtime_str.isdigit() else None
                                                else:
                                                    value = None
                                            except (ValueError, SyntaxError):
                                                # If parsing fails, try manual extraction
                                                runtime_str = value.strip('[]').strip("'\"")
                                                value = int(runtime_str) if runtime_str.isdigit() else None
                                        else:
                                            # If it's just a number as string
                                            value = int(value) if value.isdigit() else None
                                    except (ValueError, AttributeError):
                                        value = None
                                    
                            values.append(value)
                        # else:
                        #     # Use default value
                        #     values.append(default_values.get(db_col))
                    
                    # Add to batch
                    batch.append(values)
                    
                    # Insert in batches
                    if len(batch) >= batch_size:
                        try:
                            cursor.executemany(insert_stmt.as_string(self.conn), batch)
                            self.conn.commit()
                            rows_inserted += len(batch)
                            batch = []
                            print(f"Inserted batch: {rows_inserted} rows so far, skipped {skipped_rows} rows")
                        except Exception as e:
                            print(f"Error inserting batch: {e}")
                            self.conn.rollback()
                            # Process one by one to skip only problematic rows
                            for i, single_row in enumerate(batch):
                                try:
                                    cursor.execute(insert_stmt.as_string(self.conn), single_row)
                                    self.conn.commit()
                                    rows_inserted += 1
                                except Exception as e2:
                                    skipped_rows += 1
                                    if skipped_rows < 10:
                                        print(f"Error at row ~{row_num - len(batch) + i}: {e2}")
                            batch = []
                        
                except Exception as e:
                    skipped_rows += 1
                    if skipped_rows < 10:  # Limit error messages
                        print(f"Error processing row {row_num}: {e}")
                        print(f"Problematic row: {row}")
            
            # Insert any remaining rows
            if batch:
                try:
                    cursor.executemany(insert_stmt.as_string(self.conn), batch)
                    self.conn.commit()
                    rows_inserted += len(batch)
                except Exception as e:
                    print(f"Error inserting final batch: {e}")
                    self.conn.rollback()
                    # Process one by one to skip only problematic rows
                    for i, single_row in enumerate(batch):
                        try:
                            cursor.execute(insert_stmt.as_string(self.conn), single_row)
                            self.conn.commit()
                            rows_inserted += 1
                        except Exception as e2:
                            skipped_rows += 1
                            if skipped_rows < 10:
                                print(f"Error at row ~{row_num - len(batch) + i + 1}: {e2}")
        
        print(f"Total rows inserted: {rows_inserted}, skipped: {skipped_rows}")
        return rows_inserted
    
    def load_ratings_from_csv(self, csv_filename):
        """
        Load user rating data from a CSV file into the ratings table.
        
        Args:
            csv_filename: Path to the ratings CSV file
                
        Returns:
            int: Number of rows inserted
        """
        
        if not self.conn or self.conn.closed:
            self.connect()
        
        rows_inserted = 0
        skipped_rows = 0
        batch_size = 1000  # A reasonable batch size
        
        with open(csv_filename, 'r', encoding='utf-8') as csv_file:
            # Using comma as delimiter based on your example
            csv_reader = csv.DictReader(csv_file, delimiter=',')  
            
            # Process each row in the CSV
            for row_num, row in enumerate(csv_reader, start=1):
                try:
                    # Convert the CSV row to a data dictionary for insertion
                    data = {}
                    
                    # Process user_id
                    if 'user_id' in row and row['user_id']:
                        try:
                            data['user_id'] = int(row['user_id'])
                        except ValueError:
                            print(f"Invalid user_id in row {row_num}: {row['user_id']}")
                            skipped_rows += 1
                            continue
                    else:
                        print(f"Missing user_id in row {row_num}")
                        skipped_rows += 1
                        continue
                    
                    # Process movie_title_id (which will be resolved to movie_id by ensure_movie_exists)
                    if 'movie_title' in row and row['movie_title']:
                        data['movie_title_id'] = row['movie_title'].replace(" ", "+")
                    else:
                        print(f"Missing movie_title in row {row_num}")
                        skipped_rows += 1
                        continue
                    
                    # Process rating
                    if 'rating' in row and row['rating']:
                        try:
                            data['rating'] = int(row['rating'])
                        except ValueError:
                            print(f"Invalid rating in row {row_num}: {row['rating']}")
                            skipped_rows += 1
                            continue
                    else:
                        print(f"Missing rating in row {row_num}")
                        skipped_rows += 1
                        continue
                    
                    # Process timestamp
                    if 'timestamp' in row and row['timestamp']:
                        try:
                            data['updated_at'] = datetime.fromisoformat(row['timestamp'])
                        except ValueError:
                            print(f"Invalid timestamp in row {row_num}: {row['timestamp']}")
                            data['updated_at'] = datetime.now()  # Use current time as fallback
                    else:
                        data['updated_at'] = datetime.now()  # Use current time if not provided
                    
                    # Use the existing insert_record method which handles movie_title_id conversion
                    if self.insert_record('ratings', data):
                        rows_inserted += 1
                        if rows_inserted % batch_size == 0:
                            print(f"Inserted {rows_inserted} rows so far, skipped {skipped_rows} rows")
                    else:
                        skipped_rows += 1
                        print(f"Failed to insert row {row_num}: {row}")
                    
                except Exception as e:
                    skipped_rows += 1
                    print(f"Error processing row {row_num}: {e}")
                    print(f"Problematic row: {row}")
        
        print(f"Total rows inserted: {rows_inserted}, skipped: {skipped_rows}")
        return rows_inserted
    

    def load_watch_history_from_csv(self, csv_filename):
        """
        Load user watch history data from a CSV file into the watch_history table.
        
        Args:
            csv_filename: Path to the ratings CSV file
                
        Returns:
            int: Number of rows inserted
        """
        
        if not self.conn or self.conn.closed:
            self.connect()
        
        rows_inserted = 0
        skipped_rows = 0
        batch_size = 1000  # A reasonable batch size
        
        with open(csv_filename, 'r', encoding='utf-8') as csv_file:
            # Using comma as delimiter based on your example
            csv_reader = csv.DictReader(csv_file, delimiter=',')  
            
            # Process each row in the CSV
            for row_num, row in enumerate(csv_reader, start=1):
                try:
                    # Convert the CSV row to a data dictionary for insertion
                    data = {}
                    
                    # Process user_id
                    if 'user_id' in row and row['user_id']:
                        try:
                            data['user_id'] = int(row['user_id'])
                        except ValueError:
                            print(f"Invalid user_id in row {row_num}: {row['user_id']}")
                            skipped_rows += 1
                            continue
                    else:
                        print(f"Missing user_id in row {row_num}")
                        skipped_rows += 1
                        continue
                    
                    # Process movie_title_id (which will be resolved to movie_id by ensure_movie_exists)
                    if 'movie_title' in row and row['movie_title']:
                        data['movie_title_id'] = row['movie_title'].replace(" ", "+")
                    else:
                        print(f"Missing movie_title in row {row_num}")
                        skipped_rows += 1
                        continue
                    
                    # Process rating
                    if 'watched_minutes' in row and row['watched_minutes']:
                        try:
                            data['watched_minutes'] = int(row['watched_minutes'])
                        except ValueError:
                            print(f"Invalid rating in row {row_num}: {row['rating']}")
                            skipped_rows += 1
                            continue
                    else:
                        print(f"Missing watched minutes in row {row_num}")
                        skipped_rows += 1
                        continue
                    
                    # Process timestamp
                    if 'timestamp' in row and row['timestamp']:
                        try:
                            data['updated_at'] = datetime.fromisoformat(row['timestamp'])
                        except ValueError:
                            print(f"Invalid timestamp in row {row_num}: {row['timestamp']}")
                            data['updated_at'] = datetime.now()  # Use current time as fallback
                    else:
                        data['updated_at'] = datetime.now()  # Use current time if not provided
                    
                    # Use the existing insert_record method which handles movie_title_id conversion
                    if self.insert_record('watch_history', data):
                        rows_inserted += 1
                        if rows_inserted % batch_size == 0:
                            print(f"Inserted {rows_inserted} rows so far, skipped {skipped_rows} rows")
                    else:
                        skipped_rows += 1
                        print(f"Failed to insert row {row_num}: {row}")
                    
                except Exception as e:
                    skipped_rows += 1
                    print(f"Error processing row {row_num}: {e}")
                    print(f"Problematic row: {row}")
        
        print(f"Total rows inserted: {rows_inserted}, skipped: {skipped_rows}")
        return rows_inserted
    
    def get_movie_ratings_and_votes(self, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Get movie ratings and votes from the movies table where both fields are not null.
        
        Args:
            limit: Optional limit on the number of records to return
            
        Returns:
            DataFrame containing movie_id, title, rating, votes, and genres
        """
        if not self.conn or self.conn.closed:
            self.connect()
            
        query = """
            SELECT movie_id, title, rating, votes, genres
            FROM movies
            WHERE rating IS NOT NULL AND votes IS NOT NULL
        """
        
        if limit is not None:
            query += f" LIMIT {limit}"
            
        try:
            return pd.read_sql_query(query, self.conn)
        except Exception as e:
            self.logger.error(f"Error retrieving movie ratings and votes: {str(e)}")
            return pd.DataFrame(columns=['movie_id', 'title', 'rating', 'votes', 'genres'])
    
    def load_watch_chunk(self, limit: Optional[int] = None, offset: int = 0) -> pd.DataFrame:
        """
        Load a chunk of watch history data along with movie details.

        Args:
            limit: The maximum number of records to return (optional).
            offset: The number of records to skip before starting to return records.

        Returns:
            DataFrame containing user_id, movie_id, watched_minutes, title and genres.
        """
        if not self.conn or self.conn.closed:
            self.connect()
        
        query = """
            SELECT w.user_id, w.movie_id, w.watched_minutes, m.title, m.genres
            FROM watch_history w
            JOIN movies m ON w.movie_id = m.movie_id
            WHERE m.genres IS NOT NULL  -- Check if genres is not null
            ORDER BY w.updated_at
        """
        
        if limit is not None:
            query += f" LIMIT {limit} OFFSET {offset};"
        else:
            query += f" OFFSET {offset};"
        
        try:
            return pd.read_sql_query(query, self.conn)
        except Exception as e:
            self.logger.error(f"Error loading watch history chunk: {str(e)}")
            return pd.DataFrame(columns=['user_id', 'movie_id', 'watched_minutes', 'title', 'genres']) 
        
    def load_ratings_chunk(self, limit: Optional[int] = None, offset: int = 0) -> pd.DataFrame:
        """
        Load a chunk of ratings data along with movie details.

        Args:
            limit: The maximum number of records to return (optional).
            offset: The number of records to skip before starting to return records.

        Returns:
            DataFrame containing user_id, movie_id, rating, title, and genres.
        """
        if not self.conn or self.conn.closed:
            self.connect()
        
        query = f"""
            SELECT r.user_id, r.movie_id, r.rating, m.title, m.genres
            FROM ratings r
            JOIN movies m ON r.movie_id = m.movie_id
            ORDER BY r.updated_at
            LIMIT {limit} OFFSET {offset};
        """
        
        try:
            return pd.read_sql_query(query, self.conn)
        except Exception as e:
            self.logger.error(f"Error loading ratings chunk: {str(e)}")
            return pd.DataFrame(columns=['user_id', 'movie_id', 'rating', 'title', 'genres'])