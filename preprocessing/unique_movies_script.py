import pandas as pd

# Configuration
INPUT_FILE = '../data/processed_data_entries.csv'
OUTPUT_FILE = '../data/unique_movies.csv'
CHUNKSIZE = 1000000  # Adjust based on available memory

# Initialize an empty set to store unique movie titles
unique_movies_set = set()

# Process data in chunks
for chunk in pd.read_csv(INPUT_FILE, chunksize=CHUNKSIZE):
    unique_movies_set.update(chunk['movie_title'].unique())

# Convert the set of unique movie titles to a DataFrame
unique_movies_df = pd.DataFrame(list(unique_movies_set), columns=['movie_title'])

# Save the unique movies to a new file
unique_movies_df.to_csv(OUTPUT_FILE, index=False)

print(f"\nTotal unique movies: {len(unique_movies_set)}")
print(f"Unique movies have been saved to '{OUTPUT_FILE}'")
