SELECT * FROM player_seasons LIMIT 5;

SELECT COUNT(*) FROM player_seasons; 

CREATE TYPE season_stats AS (
	season INTEGER,
	gp INTEGER,
	pts REAL,
	reb REAL,
	ast REAL
);


CREATE TABLE players (
    player_name TEXT,
    height TEXT,
    college TEXT,
    country TEXT,
    draft_year TEXT,
    draft_round TEXT,
    draft_number TEXT,
    season_stats season_stats[],
    current_season INTEGER,
    PRIMARY KEY (player_name, current_season)
);

-- DROP TABLE players;

WITH yesterday AS (
	SELECT * FROM players
	WHERE current_season = 1995
),
today AS (
	SELECT * FROM player_seasons
	WHERE season = 1996
)
SELECT * FROM today t 
FULL OUTER JOIN yesterday y 
ON t.player_name = y.player_name;
-- Check out the output Right side columns are all null. Because there's nothing in the players table.


--INSERT Statment is inserted later


SELECT MIN(season) FROM player_seasons;
-- Min is 1996

INSERT INTO players
WITH yesterday AS (
	SELECT * FROM players
	WHERE current_season = 2000 --  Starting from 1995 cuz min is 1996
),
today AS (
	SELECT * FROM player_seasons
	WHERE season = 2001 -- Starting from 1995 cuz Min is 1996
)
SELECT
    COALESCE(t.player_name, y.player_name) AS player_name,
    COALESCE(t.height, y.height) AS height,
    COALESCE(t.college, y.college) AS college,
	COALESCE(t.country, y.country) AS country,
    COALESCE(t.draft_year, y.draft_year) AS draft_year,
    COALESCE(t.draft_round, y.draft_round) AS draft_round,
    COALESCE(t.draft_number, y.draft_number) AS draft_number,
	CASE 
		-- When we are starting out y will be null so create first value
		WHEN y.season_stats IS NULL 
		THEN ARRAY[ ROW(t.season, t.gp, t.pts, t.reb, t.ast):: season_stats]
		--  when player is not retired.
		WHEN t.season IS NOT NULL
		THEN y.season_stats || ARRAY[ ROW(t.season, t.gp, t.pts, t.reb, t.ast):: season_stats]
		-- When player is retired and t season is NULL keep y's season else add row
		ELSE y.season_stats
	END AS season_stats,
	COALESCE(t.season, y.current_season + 1) as current_season
FROM today t
FULL OUTER JOIN yesterday y
ON t.player_name = y.player_name; 


SELECT player_name, UNNEST(season_stats) AS season_stats 
FROM players 
WHERE player_name = 'Michael Jordan' AND current_season = 2001;

SELECT COUNT(*) FROM players WHERE current_season = 2001;
SELECT COUNT(*) FROM players; 

-- Unnested the season_stats 
WITH UNNESTED AS (
	SELECT player_name, UNNEST(season_stats) AS season_stats
	FROM players
	WHERE --player_name = 'Michael Jordan' AND 
	current_season = 2001
)

SELECT player_name, (season_stats:: season_stats).*
FROM UNNESTED;
