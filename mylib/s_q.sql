SELECT 
    movie.FILM, 
    movie.STARS, 
    movie.RATING, 
    movie.VOTES, 
    ratings.RottenTomatoes, 
    ratings.Metacritic, 
    ratings.IMDB, 
    ratings.Fandango_Stars, 
    COUNT(*) as total_rows
FROM 
    fandango_scrape movie
JOIN 
    fandango_score_comparison ratings ON movie.FILM = ratings.FILM
GROUP BY 
    movie.FILM, 
    movie.STARS, 
    movie.RATING, 
    movie.VOTES,
    ratings.RottenTomatoes, 
    ratings.Metacritic, 
    ratings.IMDB, 
    ratings.Fandango_Stars
ORDER BY 
    movie.FILM, ratings.RottenTomatoes DESC, movie.VOTES;
