# Work Sample for Data Engineer

To effectively solve the following data pipeline problems, it is essential to use a DAG (Directed Acyclic Graph) oriented tool. DAG tools like Pachyderm, Airflow, Dagster, etc., can help streamline data processing and management with tracking data lineage, ensuring data integrity, and minimizing errors during processing.

To provide more context and clarity, including pipeline specs and diagrams can be helpful. These artifacts can help visualize the DAG and its components, provide information on how data flows through the pipeline, and highlight the dependencies between tasks.

## Project Structure

*   `api.py`: A Flask API to serve the trained machine learning model.
*   `dags/airflow_dag.py`: Contains the Apache Airflow DAG defining the data processing pipeline.
*   `Dockerfile`: Defines the Docker image for Airflow, including Java for Spark compatibility.
*   `docker-compose.yaml`: Configures the multi-container Airflow environment for local development.
*   `requirements.txt`: Lists Python dependencies for the project.
*   `spark/`: Contains files related to Apache Spark integration (e.g., `spark/Dockerfile`, potentially Spark scripts).
*   `Readme.md`: This file, providing an overview of the project.
*   `.gitignore`: Specifies intentionally untracked files that Git should ignore.
*   `dag.png`: Image illustrating the Airflow DAG.
*   `logs/`: Directory for Airflow logs.

## Setup and Running the Project

*   To launch the Airflow environment, run the command: `docker-compose up --build -d`. The `-d` flag runs the containers in detached mode.
*   Once the containers are up, the Airflow UI will be accessible at `http://localhost:8080`. The default credentials (as configured in `docker-compose.yaml` during the init process) are username `airflow` and password `airflow`.
*   The `requirements.txt` file lists all Python dependencies that are installed within the Docker image.
*   The `Dockerfile` is responsible for building the Airflow image. It installs necessary system packages, such as OpenJDK (for Spark compatibility), and the Python dependencies specified in `requirements.txt`.
*   **Security Warning:** The Kaggle API credentials in `dags/airflow_dag.py` are currently hardcoded. For security, it's strongly recommended to manage these using Airflow Connections, environment variables, or a secrets management tool.

## Pipeline :

```
    tmp_data >> download_data_task >> transform_data_task_etfs >> transform_data_task_stocks >> merge_parquet_files >> model_training 
```
1. tmp_data: folder structure is created for storing data at various stages of processing. 
2. download_data_task : Downloads the data from the kaggle api end point and stores it in the tmp_data folder.
3. transform_data_task_etfs : Reads the data from the tmp_data folder and performs the necessary transformations on the data using Pandas.
4. transform_data_task_stocks : Reads the data from the tmp_data folder and performs the necessary transformations on the data using Pandas.
5. merge_parquet_files : Reads the data from the tmp_data folder and performs the necessary transformations on the data.
6. model_training : Reads the data from the tmp_data folder and performs the necessary transformations on the data.

* steps 3 and 4 could be run in parallel in my case the resources werent enough to do so. Hence ran them as sequential tasks.
* The project includes dependencies and configurations for Apache Spark (see `spark/` directory and `Dockerfile`), but the primary data processing in the active `stock_dataengineering` DAG is currently implemented with Pandas. The Spark components appear to be for future development or experimental purposes as outlined in the TODO section.

<img src="dag.png" alt="drawing" width="400"/>

## Problem 1: Raw Data Processing
1. Download the data using kaggle api end points to store and extract the contents.
2. Store the data at certain location for further data cleaning and processing.
3. Ensure necessary folders are created so the data can be written to the location further.

## Problem 2: Feature Engineering
1. Read the data from the location where it is stored.
2. Clean the data by removing the null values and duplicates.
3. Create new features from the existing features.
4. Perform rolling aggregation on the data. 
5. Perform merge operation on the parquet file so that the data is in a single file.
6. Store the resulting data for training.

## Problem 3: Integrate ML Training
1. Read the parquet files and run RandomForestRegression as suggested to train the model. 
2. Store the model in the location for further use.
 
## Problem 4: Model Serving with Flask API
1. Read the model from the location where it is stored. 
2. Create an API endpoint to serve the model. 
    *   This is implemented in `api.py`, which uses Flask to create a `/predict` endpoint.
    *   The API loads the trained model (e.g., `randomforest_model_stock_volume_prediction.sav`) and expects `vol_moving_avg` and `adj_close_rolling_med` as query parameters.
    *   To run the API (assuming the model file is present and Python + Flask are installed in the environment): `python api.py`.
    *   Example API call: `curl "http://localhost:5000/predict?vol_moving_avg=12345&adj_close_rolling_med=67.89"` (Note: `api.py` runs on port 5000 by default).
3. Model training for this was done on kaggle notebook as I couldnt train them locally due to memory constraints.
4. Couldnt complete the serving part as the model training came up to 9GB.
5. Link to notebook : https://www.kaggle.com/code/vardhan13/notebookb2cd139c81
6. Clarify model naming: The Airflow DAG's `model_training` task saves the model as `random_forest_stock_model.sav`. The `api.py` script expects to load a model named `randomforest_model_stock_volume_prediction.sav`. Ensure the correct model file, generated by the pipeline, is available to `api.py` with the expected name.

### TODO 

1. Working on integration spark clusters to the docker-compose so that Airflow can submit data intensive tasks to spark cluster.
2. Worked extensively on setting up spark but kept running into issues with the spark cluster worker getting disconnected constantly. 
3. Figure out why restarting worker is resulting in dropping the airflow scheduler, which results in failure to schedule further tasks in the piepline.
4. Standardize model file naming between the Airflow DAG and the Flask API.
5. Securely manage Kaggle API credentials.