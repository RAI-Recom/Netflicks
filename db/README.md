# **Database Schema Documentation**

### **Database Name: `netflicksdb`**

## **Tables**
### **Table: `movies`**
**Description**: Stores metadata about movies, including titles, release years, genres, ratings, and plot summaries.

| **Column Name**    | **Data Type**               | **Collation** | **Nullable** | **Default Value** | **Storage** | **Description**                     |
|---------------------|-----------------------------|---------------|--------------|-------------------|-------------|-------------------------------------|
| `movie_id`          | `integer`                  |               | `NOT NULL`   |                   | `plain`     | Unique identifier for the movie.    |
| `imdb_id`           | `integer`                  |               |              |                   | `plain`     | IMDB ID for the movie.    |
| `movie_title_id`    | `text`                     |               | `NOT NULL`   |                   | `extended`  | External or alternative title ID.   |
| `title`             | `text`                     |               |              |                   | `extended`  | The title of the movie.             |
| `year`              | `integer`                  |               |              |                   | `plain`     | The release year of the movie.      |
| `rating`            | `numeric`                  |               |              |                   | `main`      | The average rating of the movie.    |
| `genres`            | `text[]`                   |               |              |                   | `extended`  | Array of genres associated with the movie. |
| `plot`              | `text`                     |               |              |                   | `extended`  | A brief description of the plot.    |
| `duration`          | `integer`                  |               |              |                   | `plain`     | Duration of the movie in minutes.   |
| `directors`         | `text[]`                     |               |              |                   | `extended`  | Directors of the movie.              |
| `actors`            | `text[]`                     |               |              |                   | `extended`  | Cast of the movie.                  |
| `votes`             | `integer`                  |               |              |                   | `plain`     | Number of votes for the movie.      |
| `languages`         | `text[]`                   |               |              |                   | `extended`  | Array of languages associated with the movie. |
| `country`           | `text[]`                   |               |              |                   | `extended`  | Array of countries associated with the movie. |
| `release_date`      | `date`                     |               |              |                   | `plain`     | Release date of the movie.          |
| `poster`            | `text`                     |               |              |                   | `extended`  | URL of the movie poster.            |

**Relationships**:
  - Foreign Keys:
    TABLE `ratings` CONSTRAINT `ratings_movie_id_fkey` FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE
    TABLE `watch_history` CONSTRAINT `watch_history_movie_id_fkey` FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE
  - Other relationships: ``
  - Indexes:
    `movies_pkey` PRIMARY KEY, btree (movie_id)
    `movies_title_movie_title_id_key` UNIQUE CONSTRAINT, btree (title, movie_title_id)
---

### **Table: `ratings`**
**Description**: Captures user-provided ratings for movies, including the rating value, user ID, movie ID, and timestamp.

| **Column Name**    | **Data Type**               | **Collation** | **Nullable** | **Default Value**                          | **Storage** | **Description**                     |
|---------------------|-----------------------------|---------------|--------------|--------------------------------------------|-------------|-------------------------------------|
| `rating_id`         | `integer`                  |               | `NOT NULL`   | `nextval('ratings_rating_id_seq'::regclass)`| `plain`     | Unique identifier for each rating.  |
| `user_id`           | `bigint`                   |               |              |                                            | `plain`     | ID of the user who made the rating. |
| `movie_id`          | `integer`                  |               |              |                                            | `plain`     | ID of the movie being rated.        |
| `rating`            | `integer`                  |               |              |                                            | `plain`     | Rating value (e.g., 1–5 stars).     |
| `updated_at`          | `timestamp without time zone`|             |              | `now()`                                    | `plain`     | Timestamp when the rating was made. |

**Relationships**:
  - Foreign Keys:
    `ratings_movie_id_fkey` FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE
    `ratings_user_id_fkey` FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
  - Indexes:
    `ratings_pkey` PRIMARY KEY, btree (rating_id)
    Check constraints:
    `ratings_rating_check` CHECK (rating >= 1 AND rating <= 5)
---

### **Table: `users`**
**Description**: Contains information about users, uniquely identifying them with a user ID.

| **Column Name**    | **Data Type**  | **Collation** | **Nullable** | **Default Value** | **Storage** | **Description**          |
|---------------------|----------------|---------------|--------------|-------------------|-------------|--------------------------|
| `user_id`           | `bigint`       |               | `NOT NULL`   |                   | `plain`     | Unique identifier for the user. |

**Relationships**:
  - Foreign Keys:
    TABLE `ratings` CONSTRAINT `ratings_user_id_fkey` FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
    TABLE `watch_history` CONSTRAINT `watch_history_user_id_fkey` FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
  - Indexes:
    `users_pkey` PRIMARY KEY, btree (user_id)
---

### **Table: `watch_history`**
**Description**: This table tracks user interactions with movies, including when they were watched and how much time was spent watching.

| **Column Name**    | **Data Type**               | **Collation** | **Nullable** | **Default Value**                          | **Storage** | **Description**                             |
|---------------------|-----------------------------|---------------|--------------|--------------------------------------------|-------------|---------------------------------------------|
| `watch_id`          | `integer`                  |               | `NOT NULL`   | `nextval('watch_history_watch_id_seq'::regclass)` | `plain`     | Unique identifier for each watch record.    |
| `updated_at`         | `timestamp without time zone`|             |              |                                            | `plain`     | Timestamp when the watch record was created.|
| `user_id`           | `bigint`                   |               |              |                                            | `plain`     | ID of the user who watched the movie.       |
| `movie_id`          | `integer`                  |               |              |                                            | `plain`     | ID of the movie being watched.              |
| `watched_minutes`   | `integer`                  |               |              |                                            | `plain`     | Number of minutes the user watched the movie.|

**Relationships**:
  - Foreign Keys:
    `watch_history_movie_id_fkey` FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE
    `watch_history_user_id_fkey` FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
  - Indexes:
     `watch_history_pkey` PRIMARY KEY, btree (watch_id)
---

## **Functions and Triggers**

### **Function: `ensure_user_and_movie_exist`**
**Description**: This function ensures that a user and a movie exist in the database before inserting a rating.

```sql
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
```

### **Trigger: `trigger_ensure_user_movie`**
**Description**: This trigger calls the `ensure_user_and_movie_exist` function before inserting a new rating.

```sql
CREATE TRIGGER trigger_ensure_user_movie
BEFORE INSERT ON ratings
FOR EACH ROW
EXECUTE FUNCTION ensure_user_and_movie_exist();

CREATE TRIGGER trigger_ensure_user_movie_watch_history
BEFORE INSERT ON watch_history
FOR EACH ROW
EXECUTE FUNCTION ensure_user_and_movie_exist();
```

---

## **Constraints**
### **Constraint Type: Primary Key/Foreign Key/Unique/etc.**
- **Table**: `ratings`
- **Column(s)**: `rating`
- **Description**: `rating >= 1 AND rating <= 5`
---
