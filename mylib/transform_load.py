from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

def transform_load(data_path="dbfs:/FileStore/mini_project11/event_times.csv", 
                        data_path2="dbfs:/FileStore/mini_project11/serve_times.csv"):
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
    df_event_times = spark_session.read.csv(data_path, header=True, inferSchema=True)
    df_serve_times = spark_session.read.csv(data_path2, header=True, inferSchema=True)

    # Assigning unique IDs to each DataFrame
    df_event_times = df_event_times.withColumn("unique_id", monotonically_increasing_id())
    df_serve_times = df_serve_times.withColumn("unique_id", monotonically_increasing_id())

    # Saving the DataFrames as Delta tables
    df_serve_times.write.format("delta").mode("overwrite").saveAsTable("serve_times_delta_table")
    df_event_times.write.format("delta").mode("overwrite").saveAsTable("event_times_delta_table")
    
    # Counting the number of rows in each DataFrame
    total_rows_serve = df_serve_times.count()
    total_rows_event = df_event_times.count()
    print(f"Serve Times Dataset Row Count: {total_rows_serve}")
    print(f"Event Times Dataset Row Count: {total_rows_event}")
    
    return "Data transformation and storage completed successfully."

if __name__ == "__main__":
    transform_load()