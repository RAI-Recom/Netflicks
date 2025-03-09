import csv
from imdb import Cinemagoer
import time
from loguru import logger
import os

def fetch_movies_by_title(titles):
    ia = Cinemagoer()
    movies_data = []
    
    titles_count = len(titles)
    logger.info(f"Fetching data for {titles_count} movies...")

    for title in titles:
        search_results = ia.search_movie(title)
        try:
            if search_results:
                movie_id = search_results[0].movieID
                movie = ia.get_movie(movie_id)
                ia.update(movie, info=['main', 'plot', 'genres', 'ratings'])
                movies_data.append({
                    'movie_id': movie_id,
                    'movie_title_id': title.replace(" ", "+"),
                    'title': movie['title'],
                    'year': movie['year'],
                    'rating': movie.get('rating', 'N/A'),
                    'genres': ', '.join(movie.get('genres', [])),
                    'plot': movie['plot'][0].split('::')[0] if movie.get('plot') else 'N/A'
                })
                logger.info(f"({titles.index(title) + 1}/{titles_count}) Retrieved data for: {title}")
                # time.sleep(1)  # Rate limiting to avoid throttling
            else:
                logger.error(f"No results found for: {title}")
        except Exception as e:
            logger.error(f"Error fetching data for: {title}: {str(e)}")
    return movies_data

def read_movie_titles_from_csv(filepath):
    titles = []
    with open(filepath, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header row
        for row in reader:
            titles.append(row[0])  # Assuming the title is in the first column
    return titles

def write_movies_to_csv(movies, filepath):
    with open(filepath, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['movie_id', 'movie_title_id', 'title', 'year', 'rating', 'genres', 'plot'])
        for movie in movies:
            writer.writerow([movie['movie_id'], movie['movie_title_id'], movie['title'], movie['year'], movie['rating'], movie['genres'], movie['plot']])

logger.add("imdb_retrieval_faster.log")

# Read movie titles from CSV
input_csv_path = '../data/unique_movies.csv'
titles = read_movie_titles_from_csv(input_csv_path)

# Fetch movie data from IMDb
movies_data = fetch_movies_by_title(titles)

# Write fetched movie data to a new CSV file
output_csv_path = '../data/movies_metadata_faster.csv'
write_movies_to_csv(movies_data, output_csv_path)

logger.info(f"Movie data has been written to {output_csv_path}")