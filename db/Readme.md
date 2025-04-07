# **Database Schema Documentation**

## **Overview**
- **Database Name**: `netflicksdb`

---

## **Schema Diagram**

## **Tables**
### **Table: `movies`**
- **Description**: ``
- **Columns**:
     Column     |            Type             | Collation | Nullable | Default | Storage  | Compression | Stats target | Description 
----------------+-----------------------------+-----------+----------+---------+----------+-------------+--------------+-------------
 movie_id       | integer                     |           | not null |         | plain    |             |              | 
 movie_title_id | text                        |           | not null |         | extended |             |              | 
 title          | text                        |           |          |         | extended |             |              | 
 year           | integer                     |           |          |         | plain    |             |              | 
 rating         | numeric                     |           |          |         | main     |             |              | 
 genre          | text[]                      |           |          |         | extended |             |              | 
 plot           | text                        |           |          |         | extended |             |              | 
 duration       | integer                     |           |          |         | plain    |             |              | 
 created_at     | timestamp without time zone |           |          | now()   | plain    |             |              | 
 imdb_rating    | integer                     |           |          |         | plain    |             |              | 
- **Relationships**:
  - Foreign Keys:
    TABLE "ratings" CONSTRAINT "ratings_movie_id_fkey" FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE
    TABLE "watch_history" CONSTRAINT "watch_history_movie_id_fkey" FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE
  - Other relationships: ``
    Indexes:
    "movies_pkey" PRIMARY KEY, btree (movie_id)
    "movies_title_movie_title_id_key" UNIQUE CONSTRAINT, btree (title, movie_title_id)

### **Table: `ratings`**
- **Description**: ``
- **Columns**:
  Column   |            Type             | Collation | Nullable |                  Default                   | Storage | Compression | Stats target | Description 
-----------+-----------------------------+-----------+----------+--------------------------------------------+---------+-------------+--------------+-------------
 rating_id | integer                     |           | not null | nextval('ratings_rating_id_seq'::regclass) | plain   |             |              | 
 user_id   | bigint                      |           |          |                                            | plain   |             |              | 
 movie_id  | integer                     |           |          |                                            | plain   |             |              | 
 rating    | integer                     |           |          |                                            | plain   |             |              | 
 rated_at  | timestamp without time zone |           |          | now()                                      | plain   |             |              | 
- **Relationships**:
  - Foreign Keys:
    "ratings_movie_id_fkey" FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE
    "ratings_user_id_fkey" FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
  - Indexes:
    "ratings_pkey" PRIMARY KEY, btree (rating_id)
    Check constraints:
    "ratings_rating_check" CHECK (rating >= 1 AND rating <= 5)
---

### **Table: `users`**
- **Description**: ``
- **Columns**:
 Column  |  Type  | Collation | Nullable | Default | Storage | Compression | Stats target | Description 
---------+--------+-----------+----------+---------+---------+-------------+--------------+-------------
 user_id | bigint |           | not null |         | plain   |             |              | 
- **Relationships**:
  - Foreign Keys:
    TABLE "ratings" CONSTRAINT "ratings_user_id_fkey" FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
    TABLE "watch_history" CONSTRAINT "watch_history_user_id_fkey" FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
  - Indexes:
    "users_pkey" PRIMARY KEY, btree (user_id)
---

### **Table: `watch_history`**
- **Description**: ``
- **Columns**:
     Column      |            Type             | Collation | Nullable |                     Default                     | Storage | Compression | Stats target | Description 
-----------------+-----------------------------+-----------+----------+-------------------------------------------------+---------+-------------+--------------+-------------
 watch_id        | integer                     |           | not null | nextval('watch_history_watch_id_seq'::regclass) | plain   |             |              | 
 timestamp       | timestamp without time zone |           |          |                                                 | plain   |             |              | 
 user_id         | bigint                      |           |          |                                                 | plain   |             |              | 
 movie_id        | integer                     |           |          |                                                 | plain   |             |              | 
 watch_time      | timestamp without time zone |           |          |                                                 | plain   |             |              | 
 watched_minutes | integer                     |           |          |                                                 | plain   |             |              | 
- **Relationships**:
  - Foreign Keys:
    "watch_history_movie_id_fkey" FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE
    "watch_history_user_id_fkey" FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
  - Indexes:
     "watch_history_pkey" PRIMARY KEY, btree (watch_id)
---

## **Constraints**
### **Constraint Type: Primary Key/Foreign Key/Unique/etc.**
- **Table**: `ratings`
- **Column(s)**: `rating`
- **Description**: `rating >= 1 AND rating <= 5`
---
