# Mini Project 11

## Overview
"**Data Processing Pipeline**" is designed to create an effective data pipeline using Databricks, focusing on extracting, transforming, and visualizing movie rating data from Fandango. The project incorporates Python libraries and Databricks tools to facilitate efficient data handling and analysis.

### Key Components

#### Data Extraction:
- Retrieves movie rating data from Fandango via HTTP requests.
- Stores the retrieved data in the Databricks FileStore for further processing.

#### Databricks Environment Configuration:
- Sets up the Databricks environment using environment variables for authentication (e.g., `SERVER_HOSTNAME` and `ACCESS_TOKEN`).

#### Data Transformation and Storage:
- Converts the raw data into a Spark dataframe.
- Transforms the dataframe into a Delta Lake Table, stored in the Databricks environment.

#### Data Analysis and Visualization:
- Performs data analysis using Spark SQL on the transformed data.
- Visualizes the analysis results through various data visualization techniques.

#### File Path Validation for Testing:
- Includes a method to check the existence of file paths in the Databricks FileStore.
- Validates the connection to the Databricks API as a part of the pipeline's testing process.

#### Automated Trigger via GitHub Push:
- Implements an automated trigger with the Databricks API to start a pipeline run in response to a push in the GitHub repository.

### Preparation Steps:
1. Create a Databricks workspace on a cloud platform like Azure.
2. Integrate your GitHub account with the Databricks Workspace.
3. Configure a global initialization script in the Databricks cluster for environment variables.
4. Establish a Databricks cluster that supports PySpark.
5. Clone the project repository into the Databricks workspace.
6. Set up a Databricks job to automate the pipeline execution.

### Pipeline Components:
- Data Source Extraction Script: `mylib/extract.py`
- Data Transformation and Load Script: `mylib/transform_load.py`
- Data Query and Visualization Script: `mylib/query.py`

## Requirement
The project involves developing a data pipeline in Databricks, incorporating at least one data source and one data sink.

<<<<<<< Updated upstream
## Job pipeline
![etl](https://github.com/nogibjj/Mini_Project11_Yabei/assets/143656459/57c258ce-ef7f-4970-9d8c-6ec3aeca6e22)

## visualization
![result](https://github.com/nogibjj/Mini_Project11_Yabei/assets/143656459/ce851b21-99c2-44d3-8e18-5c30175aaee0)

## video


