-- Favourtie genres
SELECT 
    dm.name AS Genre, 
    COUNT(fr.ratings_id) AS Total_Ratings
FROM fact_ratings fr
JOIN dim_movies dm ON fr.movies_id = dm.movies_id
GROUP BY dm.name
ORDER BY COUNT(fr.ratings_id) DESC;

-- Ratings during the day
SELECT 
    dt.hour AS Hour, 
    COUNT(fr.ratings_id) AS Total_Ratings
FROM fact_ratings fr
JOIN dim_time dt ON fr.time_id = dt.time_id
GROUP BY dt.hour
ORDER BY dt.hour;

-- Ratings Distribution by Genre and Hour
SELECT 
    dm.name AS Genre, 
    dt.hour AS Hour, 
    COUNT(fr.ratings_id) AS Total_Ratings
FROM fact_ratings fr
JOIN dim_movies dm ON fr.movies_id = dm.movies_id
JOIN dim_time dt ON fr.time_id = dt.time_id
GROUP BY dm.name, dt.hour
ORDER BY Genre, Hour;

-- Number of ratings vs the average rating
SELECT 
    dm.name AS Movie, 
    AVG(fr.rating) AS Average_Rating, 
    COUNT(fr.ratings_id) AS Total_Ratings
FROM fact_ratings fr
JOIN dim_movies dm ON fr.movies_id = dm.movies_id
GROUP BY dm.name
HAVING COUNT(fr.ratings_id) > 50
ORDER BY Total_Ratings DESC;

-- Users with the Most Ratings
SELECT 
    du.users_id AS User_ID, 
    COUNT(fr.ratings_id) AS Total_Ratings
FROM fact_ratings fr
JOIN dim_users du ON fr.users_id = du.users_id
GROUP BY du.users_id
ORDER BY Total_Ratings DESC
LIMIT 10;

-- Number of ratings per gender
SELECT d.gender AS Pohlavie, COUNT(f.ratings_id) AS Pocet
FROM fact_ratings f
JOIN dim_users d ON d.users_id = f.users_id
GROUP BY Pohlavie;


