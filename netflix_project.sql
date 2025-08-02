-- Creating a duplicate table for working with queries
CREATE TABLE netflix_data_copied
LIKE netflix_dataset;

INSERT netflix_data_copied
SELECT *
FROM netflix_dataset;

-- Looking for the duplicate records

WITH duplicated_records as (
SELECT *, 
ROW_NUMBER() OVER (PARTITION BY show_id, `type`, title, director, country, 
date_added, release_year, rating, duration, listed_in, `description`) as row_num
FROM netflix_data_copied)

SELECT * FROM duplicated_records
WHERE row_num > 1;

-- CHECKING NULL VALUES FOR EACH COLUMNS 

SELECT *
FROM netflix_data_copied
WHERE show_id IS NULL OR show_id = ''; # no null for the show_id

SELECT *
FROM netflix_data_copied
WHERE `type` IS NULL OR `type` = ''; # no null for `type` as well

SELECT *
FROM netflix_data_copied
WHERE title is null or title = ''; # no null value for title


SELECT *
FROM netflix_data_copied
WHERE director IS NULL OR director = ''; # the director has many empty values

SELECT *
FROM netflix_data_copied
WHERE casts IS NULL OR casts = ''; # 825 records have no casts records

SELECT *
FROM netflix_data_copied
WHERE country IS NULL OR country = ''; # 831 records have no country value

SELECT *
FROM netflix_data_copied
WHERE date_added IS NULL OR date_added = ''; # 10 rows has no date

SELECT *
FROM netflix_data_copied
WHERE release_year IS NULL OR release_year=''; # no null for this 

SELECT *
FROM netflix_data_copied
WHERE rating IS NULL OR rating = ''; # 4 rows have empty value 

SELECT *
FROM netflix_data_copied
WHERE duration IS NULL OR duration = ''; # 3 records have empty values

SELECT *
FROM netflix_data_copied
WHERE listed_in IS NULL OR listed_in = ''; # no  records have empty values

SELECT *
FROM netflix_data_copied
WHERE `description` IS NULL OR `description`= ''; # no records have empty values

-- 1.  Count the Number of Movies vs TV Shows
SELECT `type`, COUNT(*) AS No_for_each_type
FROM netflix_data_copied
GROUP BY `type`;

-- 2. Find the Most Common Rating for Movies and TV Shows
SELECT `type`, rating 
FROM(
	SELECT `type`, rating, COUNT(*) content_count, 
	RANK() OVER(PARTITION BY `type` ORDER BY COUNT(*) DESC) AS `RANK`
	FROM netflix_data_copied
	WHERE rating IS NOT NULL OR rating != ''
	GROUP BY `type`, rating) AS 	t1
WHERE `RANK` = 1;

-- 3. List All Movies Released in a Specific Year (e.g., 2020)
SELECT title
FROM netflix_data_copied
WHERE `type` = 'Movie' AND release_year = '2021';

SELECT title
FROM (
	SELECT release_year , title 
	from netflix_data_copied
	WHERE `type` = 'Movie'
	GROUP BY release_year, title
	ORDER BY 1) as t2
WHERE release_year = '1988';

-- 4. Find the Top 5 Countries with the Most Content on Netflix

WITH recursive country_split AS (
	SELECT country,
		TRIM(SUBSTRING_INDEX(country,',',1)) as new_country,
        SUBSTRING(country, LENGTH(SUBSTRING_INDEX(country, ',',1)) + 2) as remaining
	FROM netflix_data_copied
    WHERE country IS NOT NULL
    
    UNION ALL
    SELECT country, 
		TRIM(SUBSTRING_INDEX(remaining, ',', 1)) as new_country,
        SUBSTRING(remaining, LENGTH(SUBSTRING_INDEX(remaining, ',',1))+2)
	FROM country_split 
    WHERE remaining IS NOT NULL AND remaining != '')
SELECT * 
FROM (
SELECT new_country, COUNT(*) as content_per_country, 
RANK() OVER(ORDER BY COUNT(*) DESC) AS `RANK`
FROM country_split
WHERE new_country != ''
GROUP BY new_country
ORDER BY 2 DESC ) AS T
WHERE `RANK` <=5;

-- 5. Identify the Longest Movie

SELECT *
FROM netflix_data_copied
WHERE CAST(SUBSTRING_INDEX(duration, ' ', 1) AS UNSIGNED) = 
		(SELECT MAX(CAST(SUBSTRING_INDEX(duration, ' ', 1)AS UNSIGNED))
				from netflix_data_copied
                where type = 'Movie' and duration like '%min');

-- 6. Find Content Added in the Last 5 Years
SELECT *
FROM netflix_data_copied
WHERE str_to_date(date_added, '%Y-%m-%d') <= CURRENT_DATE() - INTERVAL 5 YEAR;

-- 7. Find All Movies/TV Shows by Director 'Rajiv Chilaka'

WITH RECURSIVE show_director(show_id, type, title, director, casts, country, date_added,
    release_year, rating, duration, listed_in, description,
    new_director, remaining) AS(
SELECT show_id, type, title, director, casts, country, date_added,
    release_year, rating, duration, listed_in, description, 
		TRIM(SUBSTRING_INDEX(director,',',1)) AS new_director,
        SUBSTRING(director,LENGTH(SUBSTRING_INDEX(director,',',1)) +2) as remaining 
        FROM netflix_data_copied
        WHERE director IS NOT NULL
	UNION ALL
		SELECT show_id, type, title, director, casts, country, date_added,
    release_year, rating, duration, listed_in, description,
        TRIM(SUBSTRING_INDEX(remaining,',',1)) AS new_director, 
        SUBSTRING(remaining, LENGTH(SUBSTRING_INDEX(remaining,',',1))+2)
        FROM show_director
        WHERE remaining IS NOT NULL AND remaining !='')
        
	SELECT show_id, type, title, director, casts, country, date_added,
    release_year, rating, duration, listed_in, description
    FROM show_director
    WHERE new_director = 'Rajiv Chilaka' ;
    
    
    SELECT *
    FROM netflix_data_copied
    WHERE director LIKE '%Rajiv Chilaka%';
    
-- 8. List All TV Shows with More Than 5 Seasons
SELECT * 
FROM netflix_data_copied
WHERE `type` = 'TV show' AND 
		CAST(SUBSTRING_INDEX(duration, ' ' , 1) AS UNSIGNED) > 5 
        AND duration LIKE'%Seasons';

-- 9. Count the Number of Content Items in Each Genre

WITH RECURSIVE content_per_genre AS(
SELECT listed_in,
		TRIM(SUBSTRING_INDEX(listed_in,',',1))as new_genre, 
		SUBSTRING(listed_in, LENGTH(SUBSTRING_INDEX(listed_in,',',1)) + 2) as remaining
    FROM netflix_data_copied
    WHERE listed_in IS NOT NULL AND listed_in LIKE '%,%'
    
    UNION ALL
    SELECT listed_in, 
		TRIM(SUBSTRING_INDEX(remaining,',',1)) as new_genre, 
		SUBSTRING(remaining,LENGTH(SUBSTRING_INDEX(remaining,',',1))+2)
    FROM content_per_genre
    WHERE remaining IS NOT NULL AND remaining !='')
    
SELECT new_genre, COUNT(*) as no_of_shows
FROM content_per_genre
GROUP BY new_genre;
    
-- 10.Find each year and the average numbers of content release in India on netflix.
SELECT *
FROM netflix_data_copied;

WITH RECURSIVE avg_content_per_year(show_id,`type`,director,casts,country,date_added,release_year,rating,
							duration,listed_in,description,new_country,remaining) AS(
    
    SELECT show_id,`type`,director,casts,country,date_added,release_year,rating,
			duration,listed_in,description,
            TRIM(SUBSTRING_INDEX(country,',',1)) AS new_country,
            SUBSTRING(country,LENGTH(SUBSTRING_INDEX(country,',',1))+2) AS remaining
            FROM netflix_data_copied
            WHERE country IS NOT NULL
            
            UNION ALL
            
            SELECT show_id,`type`,director,casts,country,date_added,release_year,rating,
			duration,listed_in,description,
            TRIM(SUBSTRING_INDEX(remaining,',',1)) AS new_country,
            SUBSTRING(remaining,LENGTH(SUBSTRING_INDEX(remaining,',',1))+2)
            FROM avg_content_per_year
            WHERE remaining IS NOT NULL AND remaining !='')
            
SELECT release_year, COUNT(*) as content_per_year
FROM avg_content_per_year
WHERE new_country = 'India'
GROUP BY release_year
ORDER BY 2 DESC;
    
SELECT release_year,COUNT(*)
FROM netflix_data_copied
WHERE country LIKE '%India%'
GROUP BY release_year
ORDER BY 2 DESC;


-- 11. List All Movies that are Documentaries
SELECT *
FROM netflix_data_copied
WHERE `type` = 'Movie' AND listed_in LIKE '%Documentarie%';

-- 12. Find All Content Without a Director
SELECT *
FROM netflix_data_copied
WHERE director IS NULL OR director = '';

-- 13. Find How Many Movies Actor 'Salman Khan' Appeared in the Last 10 Years
SELECT *
FROM netflix_data_copied 
WHERE casts LIKE '%Salman Khan%' AND `type`='Movie'
	AND release_year > YEAR(CURRENT_DATE) - 10 ;
    
-- 14. Find the Top 10 Actors Who Have Appeared in the Highest Number of Movies Produced in India
SELECT *
FROM netflix_data_copied
WHERE `type` = 'Movie' AND country like '%India%';

WITH RECURSIVE highest_no_of_movies(show_id,`type`,director,casts,country,date_added,release_year,rating,
							duration,listed_in,description,new_casts,remaining) AS(
		SELECT show_id,`type`,director,casts,country,date_added,release_year,rating,
				duration,listed_in,description,
				TRIM(SUBSTRING_INDEX(casts,',',1)) AS new_casts,
                SUBSTRING(casts,LENGTH(SUBSTRING_INDEX(casts,',',1))+2) AS remaining
                FROM netflix_data_copied
                WHERE casts IS NOT NULL AND `type` = 'Movie' AND country LIKE '%India%'
                
                UNION ALL
                SELECT show_id,`type`,director,casts,country,date_added,release_year,rating,
					duration,listed_in,description,
				TRIM(SUBSTRING_INDEX(remaining,',',1)) AS new_casts,
                SUBSTRING(remaining,LENGTH(SUBSTRING_INDEX(remaining,',',1))+2) 
                FROM highest_no_of_movies
                WHERE remaining IS NOT NULL AND remaining !='')

SELECT *
FROM (
SELECT new_casts, COUNT(*) AS movies_played,
	DENSE_RANK() OVER(ORDER BY COUNT(*) DESC) as `rank`
FROM highest_no_of_movies
GROUP BY 1
ORDER BY 2 DESC) AS T
WHERE `rank` <=10;

/*-- 15. Categorize Content Based on the Presence of 'Kill' and 'Violence' Keywords in the description field. 
	Label content conatining these keywords as 'Bad' and all other content as 'Good'. Count the number of contents 
    falling into each categories */

SELECT Category, Count(*)
FROM(
	SELECT *, 
		CASE
		WHEN description LIKE '%Kill%' OR description like '%Violence%' THEN 'Bad'
		ELSE 
		'Good'
		END as Category
	FROM netflix_data_copied) AS category_table
GROUP BY 1;
                



