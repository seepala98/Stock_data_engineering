from pyspark.sql.functions import * # col, avg, sum, median, input_file_name, regexp_extract, expr
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
import glob
import os 

def transform_data():

    input_path = "./spark/spark/resources/data"
    output_path = "./spark/spark/resources/data/processed"

    conf = SparkConf()

    conf.setAll(
        [
            (
                "spark.master",
                os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077"),
            ),
            ("spark.driver.host", os.environ.get("SPARK_DRIVER_HOST", "local[*]")),
            ("spark.submit.deployMode", "client"),
            ('spark.ui.showConsoleProgress', 'true'),
            ("spark.driver.bindAddress", "0.0.0.0"),
            ("spark.app.name", "stock_Dataengineering")
        ]
    )

    spark = SparkSession.builder \
        .config(conf=conf).getOrCreate() 
            
    # # spark.conf.set("spark.driver.memory", "8g") 
    # # spark.conf.set("spark.executor.memory", "8g") 
    # # spark.conf.set("spark.driver.maxResultSize", "4g") 
    # # spark.conf.set("spark.master", "spark://spark-master:7077")
    # spark.conf.set("spark.sql.shuffle.partitions", "400") 
    # spark.conf.set("spark.default.parallelism", "400") 
    # spark.conf.set("spark.sql.execution.arrow.enabled", "true") 
    # spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "100000") 
    # spark.conf.set("spark.sql.execution.arrow.fallback.enabled", "true") 
    
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

    # Divide the input files into six different streams
    etf_files = glob.glob(f"{input_path}/etfs/*.csv")
    stock_files = glob.glob(f"{input_path}/stocks/*.csv")
    etf_streams = [etf_files[i:i+6] for i in range(0, len(etf_files), 6)]
    stock_streams = [stock_files[i:i+6] for i in range(0, len(stock_files), 6)]

    for i in range(6):
        # Read the ETF and stock CSV files for this stream
        df_etfs = spark.read.format("csv").option("header", "true").schema(schema).load(etf_streams[i])\
            .withColumn("Symbol", regexp_extract(input_file_name(), r"(?<=\/)(\w+)(?=\.)", 1))

        df_stocks = spark.read.format("csv").option("header", "true").schema(schema).load(stock_streams[i])\
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
        df_etfs = df_etfs.withColumn("adj_close_rolling_med", expr('percentile_approx(`Adj Close`, 0.5)').over(windowSpec))
        df_stocks = df_stocks.withColumn("adj_close_rolling_med", expr('percentile_approx(`Adj Close`, 0.5)').over(windowSpec))

        # Write the results to the output path
        df_etfs.write.mode("overwrite").parquet(f"{output_path}/etfs/stream{i}")
        df_stocks.write.mode("overwrite").parquet(f"{output_path}/stocks/stream{i}")

if __name__ == "__main__":
    transform_data()