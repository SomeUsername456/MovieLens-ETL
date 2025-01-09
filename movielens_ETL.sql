-- DROP DATABASE DOLPHIN_MOVIELENS;
CREATE DATABASE DOLPHIN_MOVIELENS;

CREATE SCHEMA DOLPHIN_MOVIELENS.staging;
USE SCHEMA DOLPHIN_MOVIELENS.staging;

-- Creating tables for staging
CREATE TABLE age_group_staging (
    age_group_id INT PRIMARY KEY,
    name VARCHAR(45)
);
CREATE TABLE genres_staging (
    genres_id INT PRIMARY KEY,
    name VARCHAR(255)
);
CREATE TABLE movies_staging (
    movies_id INT PRIMARY KEY,
    title VARCHAR(255),
    release_year CHAR(4)
);
CREATE TABLE genres_movies_staging (
    genres_movies_id INT PRIMARY KEY,
    movies_id INT,
    genres_id INT,
    FOREIGN KEY (movies_id) REFERENCES movies_staging(movies_id),
    FOREIGN KEY (genres_id) REFERENCES genres_staging(genres_id)
);
CREATE TABLE occupations_staging (
    occupations_id INT PRIMARY KEY,
    name VARCHAR(255)
);
CREATE TABLE users_staging (
    users_id INT PRIMARY KEY,
    age INT,
    gender CHAR(1),
    occupations_id INT,
    zip_code VARCHAR(255),
    FOREIGN KEY (occupations_id) REFERENCES occupations_staging(occupations_id),
    FOREIGN KEY (age) REFERENCES age_group_staging(age_group_id)
);
CREATE TABLE ratings_staging (
    ratings_id INT PRIMARY KEY,
    users_id INT,
    movies_id INT,
    rating INT,
    rated_at DATETIME,
    FOREIGN KEY (users_id) REFERENCES users_staging(users_id),
    FOREIGN KEY (movies_id) REFERENCES movies_staging(movies_id)
);
CREATE TABLE tags_staging (
    tags_id INT PRIMARY KEY,
    users_id INT,
    movies_id INT,
    tags VARCHAR(4000),
    created_at DATETIME,
    FOREIGN KEY (users_id) REFERENCES users_staging(users_id),
    FOREIGN KEY (movies_id) REFERENCES movies_staging(movies_id)
);


-- Creating DOLPHIN_MOVIELENS_STAGE for the .csv files
CREATE OR REPLACE STAGE DOLPHIN_MOVIELENS_STAGE;

LIST @DOLPHIN_MOVIELENS_STAGE;

-- Copying data into tables
COPY INTO age_group_staging
FROM @DOLPHIN_MOVIELENS_STAGE/age_group.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO genres_staging
FROM @DOLPHIN_MOVIELENS_STAGE/genres.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO movies_staging
FROM @DOLPHIN_MOVIELENS_STAGE/movies.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO genres_movies_staging
FROM @DOLPHIN_MOVIELENS_STAGE/genres_movies.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO occupations_staging
FROM @DOLPHIN_MOVIELENS_STAGE/occupations.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO users_staging
FROM @DOLPHIN_MOVIELENS_STAGE/users.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO ratings_staging
FROM @DOLPHIN_MOVIELENS_STAGE/ratings.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO tags_staging
FROM @DOLPHIN_MOVIELENS_STAGE/tags.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';


-- Creating tables from the star schema
CREATE TABLE dim_users AS
SELECT DISTINCT
    us.users_id,
    us.gender,
    age.name AS age_group,
    oc.name AS occupation
FROM users_staging us
JOIN age_group_staging age ON us.age = age.age_group_id
JOIN occupations_staging oc ON us.occupations_id = oc.occupations_id;

CREATE TABLE dim_time AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY DATE_TRUNC('HOUR', ratings.rated_at)) AS time_id,
    TO_TIMESTAMP(ratings.rated_at) AS time,
    TO_NUMBER(TO_CHAR(ratings.rated_at, 'HH24')) AS hour
FROM ratings_staging ratings
GROUP BY ratings.rated_at;

CREATE TABLE dim_date AS
SELECT
    ROW_NUMBER() OVER (ORDER BY rated_at) AS date_id,
    CAST(rated_at AS DATE) AS date,  
    DATE_PART(day, rated_at) AS day,
    DATE_PART(month, rated_at) AS month,
    DATE_PART(year, rated_at) AS year,                
    DATE_PART(week, rated_at) AS week,
    DATE_PART(dow, rated_at) + 1 AS day_of_week,        
    CASE DATE_PART(dow, rated_at) + 1
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END AS day_of_week_string,             
    CASE DATE_PART(month, rated_at)
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'June'
        WHEN 7 THEN 'July'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END AS month_string         
FROM ratings_staging
GROUP BY rated_at,
         DATE_PART(day, rated_at),
         DATE_PART(month, rated_at), 
         DATE_PART(year, rated_at), 
         DATE_PART(week, rated_at), 
         DATE_PART(dow, rated_at);

CREATE TABLE dim_movies AS
SELECT DISTINCT
    movies.movies_id,
    movies.title,
    movies.release_year,
    genres.name,
    tags.tags
FROM movies_staging movies
JOIN genres_movies_staging gm ON gm.movies_id = movies.movies_id
JOIN genres_staging genres ON genres.genres_id = gm.genres_id
LEFT JOIN tags_staging tags ON tags.movies_id = movies.movies_id;
    
CREATE TABLE fact_ratings AS
SELECT DISTINCT
       ratings.ratings_id,
       ratings.rated_at,
       ratings.rating,
       du.users_id,
       dm.movies_id,
       dd.date_id,
       dt.time_id
FROM ratings_staging ratings
JOIN dim_date dd ON CAST(ratings.rated_at AS DATE) = dd.date
JOIN dim_time dt ON TO_TIMESTAMP(ratings.rated_at) = dt.time
JOIN dim_users du ON du.users_id = ratings.users_id
JOIN dim_movies dm ON dm.movies_id = ratings.movies_id;

-- Dropping the tables used for staging
DROP TABLE IF EXISTS age_group_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS movies_staging;
DROP TABLE IF EXISTS genres_movies_staging;
DROP TABLE IF EXISTS occupations_staging;
DROP TABLE IF EXISTS users_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS tags_staging;
