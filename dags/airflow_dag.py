import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import pyarrow.parquet as pq
import zipfile
# from kaggle.api.kaggle_api_extended import KaggleApi
import os 
import opendatasets as od 
import numpy as np

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'data-engineer',
    'start_date': datetime(2023, 4, 29),
    'retries': 1,
}

# dag = DAG('raw_data_processing', default_args=default_args)
dag_path =os.getcwd()

variable_data = {
	"weather_data_tmp_directory":"/opt/airflow/data/",
	"weather_data_spark_code":"/opt/airflow/spark/spark_weather_data.py",
	"spark_dir":"/opt/airflow/spark/"
}

tmp_data_dir = variable_data["weather_data_tmp_directory"]

def _tmp_data():
    if not os.path.exists(tmp_data_dir):
        os.mkdir(tmp_data_dir)
    if not os.path.exists(f'{tmp_data_dir}stocks/'):
        os.mkdir(f'{tmp_data_dir}stocks/')
    if not os.path.exists(f'{tmp_data_dir}etfs/'):
        os.mkdir(f'{tmp_data_dir}etfs/')
    if not os.path.exists(f'{tmp_data_dir}processed/'):
        os.mkdir(f'{tmp_data_dir}processed/')
    if not os.path.exists(f'{tmp_data_dir}processed/stocks'):
        os.mkdir(f'{tmp_data_dir}processed/stocks')
    if not os.path.exists(f'{tmp_data_dir}processed/etfs'):
        os.mkdir(f'{tmp_data_dir}processed/etfs')

def download_data():
    import os 
    os.environ['KAGGLE_USERNAME'] = 'vardhan13'
    os.environ['KAGGLE_KEY'] = 'd295b2c75c11ba3154162ec543e91d54'
    print(os.getcwd())
    os.system('kaggle datasets download -d jacksoncrow/stock-market-dataset -p ./data')
    with zipfile.ZipFile('./data/stock-market-dataset.zip', 'r') as zip_ref:
        zip_ref.extractall('./data/')

def feature_engineering(category):
    input_folder = f'./data/{category}/'
    output_folder = f'./data/processed/{category}/'
    symbol_dict = pd.read_csv('./data/symbols_valid_meta.csv').set_index('Symbol')['Security Name'].to_dict()

    for file in os.listdir(input_folder):
        if file.endswith(".csv"):
            # Read CSV file and extract symbol from the file name
            df = pd.read_csv(os.path.join(input_folder, file))
            symbol = file.split('.')[0]

            # Add columns for Symbol and Security Name
            df['Symbol'] = symbol
            df['Security_Name'] = symbol_dict.get(symbol, 'Unknown')

            # Calculate the rolling average and median
            df['vol_moving_avg'] = df['Volume'].rolling(30, 1).mean()
            df['adj_close_rolling_med'] = df['Adj Close'].rolling(30, 1).median()

            # Write output to a separate CSV file for each stock/ETF
            output_file = os.path.join(output_folder, f"{symbol}.csv")
            df.to_csv(output_file, index=False)

# from pyspark.sql.functions import * # col, avg, sum, median, input_file_name, regexp_extract, expr
# from pyspark.sql.window import Window
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

# def transform_data():

#     input_path = "./data"
#     output_path = "./data/processed"

#     spark = SparkSession.builder\
#         .appName('stock_Dataengineering') \
#         .getOrCreate()
#                 # .master('local[*]') \
#         # .config("spark.driver.memory", "8g") \
    
#     # Define the schema for the CSV files
#     schema = StructType([
#         StructField("Date", StringType(), True),
#         StructField("Open", FloatType(), True),
#         StructField("High", FloatType(), True),
#         StructField("Low", FloatType(), True),
#         StructField("Close", FloatType(), True),
#         StructField("Adj Close", FloatType(), True),
#         StructField("Volume", IntegerType(), True),
#         StructField("Symbol", StringType(), True),
#     ])

#     df_etfs = spark.read.format("csv").option("header", "true").schema(schema).load(f"{input_path}/etfs/*.csv")\
#         .withColumn("Symbol", regexp_extract(input_file_name(), r"(?<=\/)(\w+)(?=\.)", 1))

#     df_stocks = spark.read.format("csv").option("header", "true").schema(schema).load(f"{input_path}/stocks/*.csv")\
#         .withColumn("Symbol", regexp_extract(input_file_name(), r"(?<=\/)(\w+)(?=\.)", 1))

#     # Read the metadata CSV file and create a PySpark DataFrame
#     df_meta = spark.read.format("csv").option("header", "true").load(f"{input_path}/symbols_valid_meta.csv")

#     df_etfs = df_etfs.join(df_meta.select("Symbol", "Security Name"), on=["Symbol"])
#     df_stocks = df_stocks.join(df_meta.select("Symbol", "Security Name"), on=["Symbol"])

#     # Calculate the 30-day moving average of the trading volume (Volume) per each stock and ETF
#     windowSpec = Window.partitionBy("Symbol").orderBy("Date").rowsBetween(-29, 0)
#     df_etfs = df_etfs.withColumn("vol_moving_avg", avg("Volume").over(windowSpec))
#     df_stocks = df_stocks.withColumn("vol_moving_avg", avg("Volume").over(windowSpec))

#     # Calculate the rolling median of the adjusted close (Adj Close) per each stock and ETF
#     windowSpec = Window.partitionBy("Symbol").orderBy("Date").rowsBetween(-29, 0)
#     # df_etfs = df_etfs.withColumn("adj_close_rolling_med", median("Adj Close").over(windowSpec))
#     # df_stocks = df_stocks.withColumn("adj_close_rolling_med", median("Adj Close").over(windowSpec))

#     df_etfs = df_etfs.withColumn("adj_close_rolling_med", expr('percentile_approx(`Adj Close`, 0.5)').over(windowSpec))
#     df_stocks = df_stocks.withColumn("adj_close_rolling_med", expr('percentile_approx(`Adj Close`, 0.5)').over(windowSpec))

#     # Write the resulting datasets into parquet files in separate directories
#     df_etfs.write.mode("overwrite").parquet(f"{output_path}/etfs")
#     df_stocks.write.mode("overwrite").parquet(f"{output_path}/stocks")
#     spark.stop()
import os
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
import joblib

def train_model(name):
    # Load data
    stocks_data = pd.concat([pd.read_csv(os.path.join('/data/processed/stocks', f)) for f in os.listdir('/data/processed/stocks') if f.endswith('.csv')])
    etfs_data = pd.concat([pd.read_csv(os.path.join('/data/processed/etfs', f)) for f in os.listdir('/data/processed/etfs') if f.endswith('.csv')])
    data = pd.concat([stocks_data, etfs_data])

    # Convert date column to datetime and set it as index
    data['Date'] = pd.to_datetime(data['Date'])
    data.set_index('Date', inplace=True)

    # Remove rows with NaN values
    data.dropna(inplace=True)

    # Select features and target
    features = ['vol_moving_avg', 'adj_close_rolling_med']
    target = 'Volume'

    X = data[features]
    y = data[target]

    # Split data into train and test sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Create a RandomForestRegressor model
    model = RandomForestRegressor(n_estimators=100, random_state=42)

    # Train the model
    model.fit(X_train, y_train)

    # Make predictions on test data
    y_pred = model.predict(X_test)

    # Calculate the Mean Absolute Error and Mean Squared Error
    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)

    # Persist the model to disk
    model_path = f'/data/models/{name}_model.pkl'
    joblib.dump(model, model_path)

    # Persist training metrics to a log file
    log_path = f'/data/logs/{name}_model.log'
    with open(log_path, 'w') as f:
        f.write(f'Mean Absolute Error: {mae}\n')
        f.write(f'Mean Squared Error: {mse}\n')


with DAG('stock_dataengineering', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    tmp_data = PythonOperator(
        task_id='tmp_data',
        python_callable=_tmp_data,
        dag=dag
    )

    download_data_task = PythonOperator(
        task_id='download_data',
        python_callable=download_data,
        dag=dag
    )

    transform_data_task_stocks = PythonOperator(
        task_id='transform_data_stocks',
        python_callable=feature_engineering,
        op_kwargs=['stocks'],
        dag=dag
    )

    transform_data_task_etfs = PythonOperator(
        task_id='transform_data_etfs',
        python_callable=feature_engineering,
        op_kwargs=['etfs'],
        dag=dag
    )

    ml_task = PythonOperator(
        task_id='train_ml',
        python_callable=train_model,
        dag=dag
    )

    tmp_data >> download_data_task >> [ transform_data_task_etfs, transform_data_task_stocks ] >> ml_task