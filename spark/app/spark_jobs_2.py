from pyspark.sql.functions import * # col, avg, sum, median, input_file_name, regexp_extract, expr
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

def transform_data():

    input_path = "./spark/spark/resources/data"
    output_path = "./spark/spark/resources/data/processed"

    spark = SparkSession.builder\
        .appName('stock_Dataengineering') \
        .getOrCreate() \
        # .config("spark.driver.memory", "8g") \
    
    # Define the schema for the CSV files
    schema = StructType([
        StructField("Date", StringType(), True),
        StructField("Open", FloatType(), True),
        StructField("High", FloatType(), True),
        StructField("Low", FloatType(), True),
        StructField("Close", FloatType(), True),
        StructField("Adj Close", FloatType(), True),
        StructField("Volume", IntegerType(), True),
        StructField("Symbol", StringType(), True),
    ])

    df_etfs = spark.read.format("csv").option("header", "true").schema(schema).load(f"{input_path}/etfs/*.csv")\
        .withColumn("Symbol", regexp_extract(input_file_name(), r"(?<=\/)(\w+)(?=\.)", 1))

    df_stocks = spark.read.format("csv").option("header", "true").schema(schema).load(f"{input_path}/stocks/*.csv")\
        .withColumn("Symbol", regexp_extract(input_file_name(), r"(?<=\/)(\w+)(?=\.)", 1))

    # Read the metadata CSV file and create a PySpark DataFrame
    df_meta = spark.read.format("csv").option("header", "true").load(f"{input_path}/symbols_valid_meta.csv")

    df_etfs = df_etfs.join(df_meta.select("Symbol", "Security Name"), on=["Symbol"])
    df_stocks = df_stocks.join(df_meta.select("Symbol", "Security Name"), on=["Symbol"])

    # Calculate the 30-day moving average of the trading volume (Volume) per each stock and ETF
    windowSpec = Window.partitionBy("Symbol").orderBy("Date").rowsBetween(-29, 0)
    df_etfs = df_etfs.withColumn("vol_moving_avg", avg("Volume").over(windowSpec))
    df_stocks = df_stocks.withColumn("vol_moving_avg", avg("Volume").over(windowSpec))

    # Calculate the rolling median of the adjusted close (Adj Close) per each stock and ETF
    windowSpec = Window.partitionBy("Symbol").orderBy("Date").rowsBetween(-29, 0)
    # df_etfs = df_etfs.withColumn("adj_close_rolling_med", median("Adj Close").over(windowSpec))
    # df_stocks = df_stocks.withColumn("adj_close_rolling_med", median("Adj Close").over(windowSpec))

    df_etfs = df_etfs.withColumn("adj_close_rolling_med", expr('percentile_approx(`Adj Close`, 0.5)').over(windowSpec))
    df_stocks = df_stocks.withColumn("adj_close_rolling_med", expr('percentile_approx(`Adj Close`, 0.5)').over(windowSpec))

    # Write the resulting datasets into parquet files in separate directories
    df_etfs.write.mode("overwrite").parquet(f"{output_path}/etfs")
    df_stocks.write.mode("overwrite").parquet(f"{output_path}/stocks")
    spark.stop()

if __name__ == "__main__":
    transform_data()