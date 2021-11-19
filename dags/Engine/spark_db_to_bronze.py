import os
import logging
from pyspark.sql import SparkSession
from airflow.hooks.base_hook import BaseHook


def load_postgres_to_bronze(table_name: str, db_conn_id: str, *args, **kwargs) -> None:
    logging.info(f"Load table DB to Bronze")
    hdfs_path = os.path.join('/', 'bronze', db_conn_id, table_name)
    pg_conn = BaseHook.get_connection(db_conn_id)
    pg_url = f"jdbc:postgresql://{pg_conn.host}:{pg_conn.port}/{pg_conn.schema}"
    pg_creds = {"user": pg_conn.login, "password": pg_conn.password}
    logging.info(f'connected to Spark')
    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath', '/home/user/shared/postgresql-42.3.1.jar') \
        .master('local') \
        .appName('postgres_to_bronze') \
        .getOrCreate()

    df = spark.read.jdbc(pg_url, table=table_name, properties=pg_creds)
    df.write.option("header", True).csv(hdfs_path, mode='overwrite')
