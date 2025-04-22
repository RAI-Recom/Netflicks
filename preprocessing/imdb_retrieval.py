import csv
from imdb import Cinemagoer
import time
from loguru import logger
import os

def fetch_movies_by_title(titles, output_csv_path):
    ia = Cinemagoer()
    
    file_exists = os.path.isfile(output_csv_path)
    titles_count = len(titles)
    
    # Read existing movies if file exists
    existing_movies = set()
    if file_exists:
        with open(output_csv_path, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)  # Skip header row
            for row in reader:
                existing_movies.add(row[1].replace("+", " "))  # Store movie titles
    
    logger.info(f"Fetching data for {titles_count} movies...")
    
    with open(output_csv_path, mode='a' if file_exists else 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(['movie_id', 'movie_title_id', 'title', 'year', 'rating', 'genres', 'plot'])
        
        for title in titles:
            # Skip if movie already exists
            if title in existing_movies:
                logger.info(f"({titles.index(title) + 1}/{titles_count}) Skipping existing movie: {title}")
                continue
                
            search_results = ia.search_movie(title)
            try:
                if search_results:
                    movie_id = search_results[0].movieID
                    movie = ia.get_movie(movie_id)
                    ia.update(movie, info=['main', 'plot', 'genres', 'ratings'])
                    writer.writerow([
                        movie_id,
                        title.replace(" ", "+"),
                        movie['title'],
                        movie['year'],
                        movie.get('rating', 'N/A'),
                        ', '.join(movie.get('genres', [])),
                        movie['plot'][0].split('::')[0] if movie.get('plot') else 'N/A'
                    ])
                    logger.info(f"({titles.index(title) + 1}/{titles_count}) Retrieved data for: {title}")
                    time.sleep(2)  # Add delay between requests to avoid rate limiting
                else:
                    logger.error(f"No results found for: {title}")
            except Exception as e:
                logger.error(f"An error occurred while processing: {title}: {e}")
                time.sleep(5)  # Longer delay after an error

def read_movie_titles_from_csv(filepath):
    titles = []
    with open(filepath, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header row
        for row in reader:
            titles.append(row[0])  # Assuming the title is in the first column
    return titles

logger.add("imdb_retrieval.log")

# Read movie titles from CSV
input_csv_path = '../data/unique_movies.csv'
titles = read_movie_titles_from_csv(input_csv_path)

# Fetch movie data from IMDb and write to CSV incrementally
output_csv_path = '../data/movies_metadata.csv'
fetch_movies_by_title(titles, output_csv_path)

logger.info(f"Movie data has been written to {output_csv_path}")