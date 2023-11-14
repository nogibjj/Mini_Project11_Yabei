SELECT 
    m.FILM, 
    m.STARS, 
    m.RATING, 
    m.VOTES, 
    r.RottenTomatoes, 
    r.Metacritic, 
    r.IMDB, 
    r.Fandango_Stars, 
    COUNT(*) as total_entries 
FROM 
    fandango_scrape_delta_table m 
JOIN 
    fandango_score_delta_table r ON m.FILM = r.FILM 
GROUP BY 
    m.FILM, 
    m.STARS, 
    m.RATING, 
    m.VOTES, 
    r.RottenTomatoes, 
    r.Metacritic, 
    r.IMDB, 
    r.Fandango_Stars 
ORDER BY 
    m.FILM, 
    r.RottenTomatoes DESC, 
    m.VOTES;