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
        "m.FILM, "
        "m.STARS, "
        "m.RATING, "
        "m.VOTES, "
        "r.RottenTomatoes, "
        "r.Metacritic, "
        "r.IMDB, "
        "r.Fandango_Stars, "
        "COUNT(*) as total_entries "
        "FROM fandango_scrape_delta_table m "
        "JOIN fandango_score_delta_table r ON m.FILM = r.FILM "
        "GROUP BY "
        "m.FILM, "
        "m.STARS, "
        "m.RATING, "
        "m.VOTES, "
        "r.RottenTomatoes, "
        "r.Metacritic, "
        "r.IMDB, "
        "r.Fandango_Stars "
        "ORDER BY "
        "m.FILM, "
        "r.RottenTomatoes DESC, "
        "m.VOTES"
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

    # plt.figure(figsize=(12, 7))
    # data_frame.select("VOTES", "FILM").toPandas().boxplot(
    #     column="VOTES", by="FILM"
    # )
    # plt.xlabel("Film Title")
    # plt.ylabel("Number of Votes")
    # plt.title("Distribution of Votes Across Films")
    # plt.xticks(rotation=25)
    # plt.tight_layout()
    # plt.show("film_votes_distribution.png")

    avg_seconds_surface = data_frame.groupBy("RATING").avg(
        "VOTES"
    )
    df_avg_surface = avg_seconds_surface.toPandas()

    plt.figure(figsize=(10, 5))
    plt.bar(
        df_avg_surface["RATING"],
        df_avg_surface["avg(VOTES)"],
        color="green"
    )
    plt.xlabel("Rating Type")
    plt.ylabel("Average Number of Votes")
    plt.title("Average Votes by Rating Type")
    plt.xticks(rotation=30)
    plt.show("rating_votes_analysis.png")


if __name__ == "__main__":
    visualize_data()