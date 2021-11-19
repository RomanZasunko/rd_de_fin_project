import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as spark_func
from sqlalchemy.sql.functions import mode
import logging


def load_db_bronze_to_silver(table: str, project: str, *args, **kwargs) -> None:
    logging.info("Load table to silver layer")
    raw_path = os.path.join('/', 'bronze', project, table)
    clean_path = os.path.join('/', 'silver', project, table)
    spark = SparkSession.builder \
        .master('local') \
        .appName('bronze_to_silver') \
        .getOrCreate()
    logging.info(f"extract data table to dataframe")
    df = spark.read \
        .option('header', True) \
        .option('inferSchema', True) \
        .csv(raw_path)
    logging.info("End read data from table to  dataframe")
    logging.info(f"write data table to silver layer")
    df.write.parquet(clean_path, mode='overwrite')


def load_bronze_dsshop_api_orders_to_silver(*args, **kwargs) -> None:
    logging.info("Load table to silver layer")

    raw_path = os.path.join('/', 'bronze', 'dshop', 'orders')
    clean_path = os.path.join('/', 'silver', 'dshop', 'orders')
    spark = SparkSession.builder \
        .master('local') \
        .appName('bronze_to_silver') \
        .getOrCreate()

    logging.info("Begin of read data for table orders to spark dataframe")
    df = spark.read \
        .option('header', True) \
        .option('inferSchema', True) \
        .csv(raw_path)

    logging.info(f"Begin of transform data")
    df = df.groupby(spark_func.col("order_id"), spark_func.col("product_id"), spark_func.col("client_id"),
                    spark_func.col("store_id"), spark_func.col("order_date")) \
        .agg(spark_func.sum(spark_func.col('quantity'))) \
        .select(spark_func.col("order_id"), spark_func.col("product_id"), spark_func.col("client_id"),
                spark_func.col("store_id"), spark_func.col("order_date"),
                spark_func.col('sum(quantity)').alias('quantity'))
    logging.info("End of transform data")

    logging.info(f"write data to Silver")
    df.write.parquet(clean_path, mode='overwrite')
