import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import pyarrow.parquet as pq
import zipfile
import os 
import numpy as np

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


default_args = {
    'owner': 'data-engineer',
    'start_date': datetime(2023, 4, 29),
    'retries': 1,
}

# dag = DAG('raw_data_processing', default_args=default_args)
dag_path =os.getcwd()

variable_data = {
	"weather_data_tmp_directory":"/opt/airflow/spark/spark/resources/data/",
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
    os.environ['KAGGLE_USERNAME'] = '<username>'
    os.environ['KAGGLE_KEY'] = '<key>'
    print(os.getcwd())
    os.system('kaggle datasets download -d jacksoncrow/stock-market-dataset -p ./spark/spark/resources/data/')
    with zipfile.ZipFile('./spark/spark/resources/data/stock-market-dataset.zip', 'r') as zip_ref:
        zip_ref.extractall('./spark/spark/resources/data/')

import dask.dataframe as dd
import fastparquet as fp

def feature_engineering(category):
    input_folder = f'./spark/spark/resources/data/{category}/'
    output_file = f'./spark/spark/resources/data/processed/{category}.parquet'
    symbol_dict = pd.read_csv('./spark/spark/resources/data/symbols_valid_meta.csv').set_index('Symbol')['Security Name'].to_dict()

    df_list = []
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

            df_list.append(df)

    # Concatenate all the dataframes 
    df_final = pd.concat(df_list,ignore_index= True)
    df_final.to_parquet(output_file,index = False)

# merge both the parquet files into one 
import pyarrow.parquet as pq 
import pandas as pd
import os

def merge_parquet_files(): 
    stocks = pq.ParquetDataset('./spark/spark/resources/data/processed/stocks.parquet').read_pandas().to_pandas()
    etfs = pq.ParquetDataset('./spark/spark/resources/data/processed/etfs.parquet').read_pandas().to_pandas()
    df_final = pd.concat([stocks, etfs], ignore_index=True)
    df_final.to_parquet('./spark/spark/resources/data/processed/stocks_etfs.parquet', index = False)

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
import pickle
import pandas as pd 

def model_training_Stocks():
    stock_etfs_path = "./spark/spark/resources/data/processed/stocks_etfs.parquet"
    data = pd.read_parquet(stock_etfs_path)
    
    data['Date'] = pd.to_datetime(data['Date'])
    data.set_index('Date', inplace=True)

    data.dropna(inplace=True)
    Data1 = data.sample(frac = 0.05, random_state=42)

    # Select features and target
    features = ['vol_moving_avg', 'adj_close_rolling_med']
    target = 'Volume'

    X = Data1[features]
    y = Data1[target]

    # Split data into train and test sets
    X_train, X_test, y_train, y_test = train_test_split(X.values, y.values, test_size=0.2, random_state=42)

    # Create a RandomForestRegressor model
    model = RandomForestRegressor(n_estimators=100, random_state=42)

    # Train the model
    model.fit(X_train, y_train)

    # Train the model
    y_pred = model.predict(X_test)

    # Calculate the Mean Absolute Error and Mean Squared Error
    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)

    # Saving the trained model.
    model_save_path = "./random_forest_stock_model.sav"
    pickle.dump(model, open(model_save_path, 'wb'))
  

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
        op_args=['stocks'],
        dag=dag
    )

    transform_data_task_etfs = PythonOperator(
        task_id='transform_data_etfs',
        python_callable=feature_engineering,
        op_args=['etfs'],
        dag=dag
    )

    merget_parquet_files = PythonOperator(
        task_id='merge_parquet_files',
        python_callable=merge_parquet_files,
        dag=dag
    )

    model_training = PythonOperator(
        task_id = 'model_training',
        python_callable=model_training_Stocks,
        dag=dag
    )
 
        