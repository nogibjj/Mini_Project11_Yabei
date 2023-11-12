import requests
import os
import json
import base64
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
hostname = os.getenv("SERVER_HOSTNAME")
bearer_token = os.getenv("ACCESS_TOKEN")
DBFS_PATH = "dbfs:/FileStore/tables/mini11"
auth_headers = {'Authorization': f'Bearer {bearer_token}'}
base_url = f"https://{hostname}/api/2.0"


def send_request(endpoint, headers, payload={}):
    with requests.Session() as sess:
        response = sess.post(f'{base_url}{endpoint}', 
                             data=json.dumps(payload), 
                             headers=headers, 
                             verify=True)
    return response.json()


def create_directory(directory_path, headers):
    params = {'path': directory_path}
    return send_request('/dbfs/mkdirs', headers, params)
  

def initialize_file(path, should_overwrite, headers):
    params = {'path': path, 'overwrite': should_overwrite}
    return send_request('/dbfs/create', headers, params)


def append_data(file_handle, block_data, headers):
    params = {'handle': file_handle, 'data': block_data}
    return send_request('/dbfs/add-block', headers, params)


def finalize_file(file_handle, headers):
    params = {'handle': file_handle}
    return send_request('/dbfs/close', headers, params)


def upload_file_from_web(source_url, target_dbfs_path, overwrite_flag, headers):
    web_response = requests.get(source_url)
    if web_response.status_code == 200:
        file_content = web_response.content
        file_handle = initialize_file(target_dbfs_path, overwrite_flag, headers)['handle']
        print(f"Uploading to: {target_dbfs_path}")
        for segment in range(0, len(file_content), 2**20):
            encoded_data = base64.b64encode(file_content[segment:segment + 2**20]).decode()
            append_data(file_handle, encoded_data, headers)
        finalize_file(file_handle, headers)
        print(f"Successfully uploaded {target_dbfs_path}.")
    else:
        print(f"Failed to download from {source_url}. Error: {web_response.status_code}")


def extract(
        source_url1="https://raw.githubusercontent.com/fivethirtyeight/data/master/fandango/fandango_score_comparison.csv",
        source_url2="hhttps://raw.githubusercontent.com/fivethirtyeight/data/master/fandango/fandango_scrape.csv",
        target_path1=DBFS_PATH+"/fandango_score_comparison.csv",
        target_path2=DBFS_PATH+"/fandango_scrape.csv",
        destination_directory=DBFS_PATH,
        overwrite_existing=True
):
    create_directory(destination_directory, auth_headers)
    upload_file_from_web(source_url1, target_path1, overwrite_existing, auth_headers)
    upload_file_from_web(source_url2, target_path2, overwrite_existing, auth_headers)

    return target_path1, target_path2


if __name__ == "__main__":
    data_extract()
