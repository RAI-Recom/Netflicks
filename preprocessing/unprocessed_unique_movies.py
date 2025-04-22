import csv

def find_unprocessed_movies(unique_movies_path, movies_metadata_path, output_path):
    # Read processed movies from metadata
    processed_movies = set()
    with open(movies_metadata_path, 'r', encoding='utf-8') as metadata_file:
        metadata_reader = csv.reader(metadata_file)
        next(metadata_reader)  # Skip header
        for row in metadata_reader:
            # Assuming the second column contains the original movie title
            processed_movies.add(row[1].replace('+', ' '))

    # Read unique movies and filter unprocessed
    unprocessed_movies = []
    with open(unique_movies_path, 'r', encoding='utf-8') as unique_movies_file:
        unique_reader = csv.reader(unique_movies_file)
        headers = next(unique_reader)  # Save original headers
        
        for row in unique_reader:
            movie_title = row[0]  # Assuming movie title is in first column
            if movie_title not in processed_movies:
                unprocessed_movies.append(row)

    # Write unprocessed movies to new CSV
    with open(output_path, 'w', newline='', encoding='utf-8') as output_file:
        writer = csv.writer(output_file)
        writer.writerow(headers)  # Write original headers
        writer.writerows(unprocessed_movies)

    print(f"Unprocessed movies saved to {output_path}")
    print(f"Total unprocessed movies: {len(unprocessed_movies)}")

# Paths to your files
unique_movies_path = '../data/unique_movies.csv'
movies_metadata_path = '../data/movies_metadata.csv'
output_path = '../data/unique_movies_unprocessed.csv'

# Run the script
find_unprocessed_movies(unique_movies_path, movies_metadata_path, output_path)