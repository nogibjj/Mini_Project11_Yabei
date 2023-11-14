from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

def transform_load(data_path="dbfs:/FileStore/tables/mini11/fandango_score_comparison-1.csv", 
                        data_path2="dbfs:/FileStore/tables/mini11/fandango_scrape.csv"):
    """
    Loads, transforms, and stores CSV datasets using Spark.
    
    Args:
    data_path (str): Path to the event times CSV file.
    data_path2 (str): Path to the serve times CSV file.
    
    Returns:
    str: Confirmation message after completing the data transformation and storage.
    """
    spark_session = SparkSession.builder.appName("CSVDataLoader").getOrCreate()

    # Reading and transforming CSV datasets
    df_fandango_score = spark_session.read.csv(data_path, header=True, inferSchema=True)
    df_fandango_scrape = spark_session.read.csv(data_path2, header=True, inferSchema=True)

    # Assigning unique IDs to each DataFrame
    df_fandango_score = df_fandango_score.withColumn("unique_id", monotonically_increasing_id())
    df_fandango_scrape = df_fandango_scrape.withColumn("unique_id", monotonically_increasing_id())

    # Saving the DataFrames as Delta tables
    df_fandango_scrape.write.format("delta").mode("overwrite").saveAsTable("fandango_scrape_delta_table")
    df_fandango_score.write.format("delta").mode("overwrite").saveAsTable("fandango_score_delta_table")
    
    # Counting the number of rows in each DataFrame
    total_rows_scrape = df_fandango_scrape.count()
    total_rows_score = df_fandango_score.count()
    print(f"Serve Times Dataset Row Count: {total_rows_scrape}")
    print(f"Event Times Dataset Row Count: {total_rows_score}")
    
    return "Data transformation and storage completed successfully."

if __name__ == "__main__":
    transform_load()
