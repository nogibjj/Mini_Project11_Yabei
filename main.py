from mylib.extract import extract
from mylib.transform_load import transform_load
from mylib.query import sql_query, visualize_data
import os 


if __name__ == "__main__":
    current_directory = os.getcwd()
    print(current_directory)
    extract()
    transform_load()
    sql_query()
    visualize_data()