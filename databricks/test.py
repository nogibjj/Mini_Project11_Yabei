# Databricks notebook source
!pip install -r ../requirements.txt

# COMMAND ----------

import requests
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
import json
import base64

# List files in DBFS root
display(dbutils.fs.ls('dbfs:/'))

# COMMAND ----------

# Load environment variables
load_dotenv()
hostname = os.getenv("SERVER_HOSTNAME")
token = os.getenv("ACCESS_TOKEN")

# COMMAND ----------

# Configure headers for HTTP requests
http_headers = {'Authorization': f'Bearer {token}'}
api_url = f"https://{hostname}/api/2.0"

# Function to perform HTTP POST requests
def send_post_request(api_path, headers, payload={}):
    with requests.Session() as session:
        response = session.post(f'{api_url}{api_path}', data=json.dumps(payload), verify=True, headers=headers)
    return response.json()

# Create directories in DBFS
def create_directory(dbfs_path, headers):
    data = {'path': dbfs_path}
    return send_post_request('/dbfs/mkdirs', headers, data)

# Check and create a new directory
create_directory("dbfs:/FileStore/nlp", http_headers)

# Functions to handle file operations in DBFS
def initiate_file(path, overwrite_flag, headers):
    data = {'path': path, 'overwrite': overwrite_flag}
    return send_post_request('/dbfs/create', headers, data)

def write_block(handle, block, headers):
    data = {'handle': handle, 'data': block}
    return send_post_request('/dbfs/add-block', headers, data)

def finalize_file(handle, headers):
    data = {'handle': handle}
    return send_post_request('/dbfs/close', headers, data)

def upload_local_file(source, dbfs_target, overwrite, headers):
    file_handle = initiate_file(dbfs_target, overwrite, headers)['handle']
    print(f"Uploading file to: {dbfs_target}")
    with open(source, 'rb') as file:
        while True:
            chunk = file.read(2**20)
            if not chunk:
                break
            write_block(file_handle, base64.standard_b64encode(chunk).decode(), headers)
        finalize_file(file_handle, headers)

def upload_from_web(source_url, dbfs_target, overwrite, headers):
    web_response = requests.get(source_url)
    if web_response.status_code == 200:
        content = web_response.content
        file_handle = initiate_file(dbfs_target, overwrite, headers)['handle']
        print(f"Uploading file to: {dbfs_target}")
        for i in range(0, len(content), 2**20):
            write_block(file_handle, base64.standard_b64encode(content[i:i+2**20]).decode(), headers)
        finalize_file(file_handle, headers)
        print(f"File {dbfs_target} successfully uploaded.")
    else:
        print(f"Download error from {source_url}. Status code: {web_response.status_code}")

# Usage of the upload function
serve_times_web_url = "https://github.com/fivethirtyeight/data/blob/master/tennis-time/serve_times.csv?raw=true"
serve_times_dbfs_path = "dbfs:/FileStore/nlp/test_times.csv"
overwrite_file = True

upload_from_web(serve_times_web_url, serve_times_dbfs_path, overwrite_file, http_headers)

# COMMAND ----------

# Display files in the specified DBFS directory
display(dbutils.fs.ls('dbfs:/FileStore/nlp/'))

# Check if a file exists in DBFS
print(os.path.exists('dbfs:/FileStore/nlp'))

# COMMAND ----------

# Initialize Spark session for reading CSV files
spark_session = SparkSession.builder.appName("CSVReader").getOrCreate()

# Define path for the test times CSV file
test_times_csv_path = "dbfs:/FileStore/nlp/test_times.csv"

# Read the CSV file into a DataFrame
df_test_times = spark_session.read.csv(test_times_csv_path, header=True, inferSchema=True)

# Display DataFrame and count rows
df_test_times.show()
print(f"Number of rows in test_times_df: {df_test_times.count()}")

# Save the DataFrame as a Delta table
df_test_times.write.format("delta").mode("overwrite").saveAsTable("test_times_delta")

# COMMAND ----------

# Read and display data from the Delta table
delta_df = spark_session.read.table("test_times_delta")
delta_df.show()
print(f"Number of rows in delta_df: {delta_df.count()}")

# COMMAND ----------

# Additional code blocks and functionality can be added below
