import requests
import os
from dotenv import load_dotenv

# Initialize environment variables
load_dotenv()
databricks_host = os.getenv("SERVER_HOSTNAME")
bearer_token = os.getenv("ACCESS_TOKEN")
FILESTORE_PATH = "dbfs:/FileStore/tables/mini11"
api_endpoint = f"https://{databricks_host}/api/2.0"

# Function to validate the existence of a path in DBFS
def test_validate_dbfs_path(): 
    http_headers = {'Authorization': f'Bearer {bearer_token}'}
    try:
        dbfs_response = requests.get(f"{api_endpoint}/dbfs/get-status?path={FILESTORE_PATH}", headers=http_headers)
        dbfs_response.raise_for_status()
        return 'path' in dbfs_response.json()
    except Exception as error:
        print(f"Encountered error while verifying DBFS path: {error}")
        return False

# Function to test the functionality of Databricks configuration
def test_run_databricks():
    assert test_validate_dbfs_path(), "DBFS path does not exist or cannot be accessed"

if __name__ == "__main__":
    test_run_databricks()
