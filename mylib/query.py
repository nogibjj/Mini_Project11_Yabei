from pyspark.sql import SparkSession
import matplotlib.pyplot as plt


# Function to execute a SQL query on Spark DataFrame
def sql_query():
    """
    Execute a specific SQL query using Spark.

    Returns:
        DataFrame: The result of the executed query.
    """
    spark_session = SparkSession.builder.appName("DataQuerySession").getOrCreate()
    sql_query = (
    "SELECT "
    "movie.FILM, "
    "movie.STARS, "
    "movie.RATING, "
    "movie.VOTES, "
    "ratings.RottenTomatoes, "
    "ratings.Metacritic, "
    "ratings.IMDB, "
    "ratings.Fandango_Stars, "
    "COUNT(*) as total_entries "
    "FROM fandango_scrape movie "
    "JOIN fandango_score_comparison ratings ON movie.FILM = ratings.FILM "
    "GROUP BY "
    "movie.FILM, "
    "movie.STARS, "
    "movie.RATING, "
    "movie.VOTES, "
    "ratings.RottenTomatoes, "
    "ratings.Metacritic, "
    "ratings.IMDB, "
    "ratings.Fandango_Stars "
    "ORDER BY "
    "movie.FILM, "
    "ratings.RottenTomatoes DESC, "
    "movie.VOTES"
)

    result_set = spark_session.sql(sql_query)
    return result_set


# Function to visualize the query results
def visualize_data():
    data_frame = sql_query()
    row_count = data_frame.count()
    if row_count > 0:
        print(f"Data validation successful. Found {row_count} rows.")
    else:
        print("Data unavailable. Further investigation required.")

    plt.figure(figsize=(12, 7))
    data_frame.select("seconds_before_next_point", "server").toPandas().boxplot(
        column="seconds_before_next_point", by="server"
    )
    plt.xlabel("Server Identity")
    plt.ylabel("Timing (Seconds)")
    plt.title("Distribution of Seconds Before Next Point Across Servers")
    plt.xticks(rotation=25)
    plt.tight_layout()
    plt.show("server_distribution.png")

    avg_seconds_surface = data_frame.groupBy("surface").avg(
        "seconds_before_next_point"
    )
    df_avg_surface = avg_seconds_surface.toPandas()

    plt.figure(figsize=(10, 5))
    plt.bar(
        df_avg_surface["surface"],
        df_avg_surface["avg(seconds_before_next_point)"],
        color="green"
    )
    plt.xlabel("Surface Type")
    plt.ylabel("Average Time (Seconds)")
    plt.title("Average Time Before Next Point by Surface Type")
    plt.xticks(rotation=30)
    plt.show("surface_analysis.png")


if __name__ == "__main__":
    visualize_data()
