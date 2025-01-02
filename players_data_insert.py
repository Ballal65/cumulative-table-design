def cumulatively_increment_players(spark, player_seasons, players, min_season, max_season):
    # Ensure `season` in player_seasons is an integer
    player_seasons = player_seasons.withColumn("season", player_seasons["season"].cast("int"))
    
    player_seasons.createOrReplaceTempView("player_seasons")
    players.createOrReplaceTempView("players")
    
    # Iterate over each season
    for i in range(min_season, max_season + 1):
        current_season = i - 1
        season = i
        print(f"Current Season in players = {current_season} \n season {season}")
        # SparkSQL query
        cumulative_query = f"""
        WITH yesterday AS (
            SELECT * FROM players
            WHERE current_season = {current_season}
        ),
        today AS (
            SELECT * FROM player_seasons
            WHERE season = {season}
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
                WHEN y.season_stats IS NULL THEN array(named_struct('season', t.season, 'gp', t.gp, 'pts', t.pts, 'reb', t.reb, 'ast', t.ast))
                WHEN t.season IS NOT NULL THEN concat(y.season_stats, array(named_struct('season', t.season, 'gp', t.gp, 'pts', t.pts, 'reb', t.reb, 'ast', t.ast)))
                ELSE y.season_stats
            END AS season_stats,
            COALESCE(t.season, y.current_season + 1) as current_season
        FROM today t
        FULL OUTER JOIN yesterday y
        ON t.player_name = y.player_name
        """
        
        # Execute the query
        updated_players = spark.sql(cumulative_query)
        
        # Append the new results to `players` (cumulative union)
        players = players.union(updated_players)
    
    return players


if __name__ == '__main__':
    spark = SparkSession.builder.master("local").appName("players_data_insert").getOrCreate()
    player_seasons = spark.read.option("header", "true").csv("../../../../iceberg/data/player_seasons.csv")
    season_stats_schema = StructType([
    StructField("season", IntegerType(), True),
    StructField("gp", IntegerType(), True),
    StructField("pts", FloatType(), True),
    StructField("reb", FloatType(), True),
    StructField("ast", FloatType(), True)
	])
	players_schema = StructType([
    StructField("player_name", StringType(), True),
    StructField("height", StringType(), True),
    StructField("college", StringType(), True),
    StructField("country", StringType(), True),
    StructField("draft_year", StringType(), True),
    StructField("draft_round", StringType(), True),
    StructField("draft_number", StringType(), True),
    StructField("season_stats", ArrayType(season_stats_schema), True),  # Array of season_stats
    StructField("current_season", IntegerType(), True)
	])
	players = spark.createDataFrame([],players_schema)
	output_df = cumulatively_increment_players(spark, player_seasons, players, 1996, 2022)
	output_df.write.mode("overwrite").insertInto("players")