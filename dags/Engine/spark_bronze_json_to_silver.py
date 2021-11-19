import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType,
                               StringType,
                               LongType)


def load_data_from_bronze_to_silver(project: str, *args, **kwargs):
    logging.info(f"Begin load data to silver")

    raw_path = os.path.join('/', 'bronze', project, '*', '*', '*')
    clean_path = os.path.join('/', 'silver', project)

    spark = SparkSession.builder \
        .master('local') \
        .appName('bronze_to_silver') \
        .getOrCreate()

    json_schema = StructType() \
        .add('date', StringType()) \
        .add('product_id', LongType())
    df = spark.read.schema(json_schema).json(raw_path)

    logging.info(f"clean dublicates")
    df = df.dropDuplicates()
    df.write.parquet(clean_path, mode='overwrite')
    logging.info(f"Writed data to parquet")
