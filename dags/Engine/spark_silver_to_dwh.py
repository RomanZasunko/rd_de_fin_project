from pyspark.sql import SparkSession
from pyspark.sql import functions as spark_function
from airflow.hooks.base_hook import BaseHook
import logging


def load_dim_clients_to_dwh(*args, **kwargs) -> None:
    dimension = "dim_client"
    logging.info('connectd to Spark')
    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath', '/home/user/shared/postgresql-42.3.1.jar') \
        .master('local') \
        .appName('sales_to_dwh') \
        .getOrCreate()

    logging.info(f"Begin get DWH database connection info from connection config")
    CONN = BaseHook.get_connection('DWH')
    URL = f"jdbc:postgresql://{CONN.host}:{CONN.port}/{CONN.schema}"
    CRED = {"user": CONN.login, "password": CONN.password}
    DATA_PATH = "/silver/dshop/"
    TABLE = "clients"
    DATA_PATH = DATA_PATH + TABLE

    df_silver_clients = spark.read.parquet(DATA_PATH)

    logging.info('prepeare data for dimension')
    df_dim_clients = df_silver_clients \
        .select('client_id', spark_function.col('fullname').alias('client_name'))
    logging.info(f"Begin of write dimension: {dimension}")
    df_dim_clients.write.jdbc(URL, table='dim_clients', properties=CRED, mode='overwrite')


def load_dimmension_to_dwh(*args, **kwargs) -> None:
    dimension = "dim_products"
    logging.info("connected to spark")

    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath', '/home/user/shared/postgresql-42.3.1.jar') \
        .master('local') \
        .appName('sales_to_dwh') \
        .getOrCreate()

    logging.info(f"connected to DWH")
    CONN = BaseHook.get_connection('DWH')
    URL = f"jdbc:postgresql://{CONN.host}:{CONN.port}/{CONN.schema}"
    CRED = {"user": CONN.login, "password": CONN.password}

    DATA_PATH = "/silver/dshop/"
    TABLE = "aisles"
    DATA_PATH = DATA_PATH + TABLE
    df_silver_aisles = spark.read.parquet(DATA_PATH)

    TABLE = "departments"
    DATA_PATH = "/silver/dshop/"
    DATA_PATH = DATA_PATH + TABLE

    logging.info(f" read data from table to  dataframe")
    df_silver_departments = spark.read.parquet(DATA_PATH)

    TABLE = "products"
    DATA_PATH = "/silver/dshop/"
    DATA_PATH = DATA_PATH + TABLE

    logging.info(f" read data from table to  dataframe")
    df_silver_products = spark.read.parquet(DATA_PATH)

    logging.info(f"prepeare data for demension")
    df_dim_products = df_silver_products \
        .join(df_silver_aisles, 'aisle_id', 'left') \
        .join(df_silver_departments, 'department_id', 'left') \
        .select('product_id', 'product_name', 'aisle', 'department')
    logging.info("write data")
    df_dim_products.write.jdbc(URL, table='dim_products', properties=CRED, mode='overwrite')


def load_dimension_date_to_dwh(*args, **kwargs) -> None:
    logging.info("connected to spark")
    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath', '/home/user/shared/postgresql-42.3.1.jar') \
        .master('local') \
        .appName('sales_to_dwh') \
        .getOrCreate()

    logging.info(f"connected to DWH")
    CONN = BaseHook.get_connection('DWH')
    URL = f"jdbc:postgresql://{CONN.host}:{CONN.port}/{CONN.schema}"
    CREDS = {"user": CONN.login, "password": CONN.password}

    logging.info("prepearing dimension data")
    df_period = spark.createDataFrame(["2000-01-01"], "string").toDF("start")
    df_period = df_period.withColumn("stop", spark_function.current_date())
    start, stop = df_period.select([spark_function.col(c).cast("timestamp").cast("long") for c in ("start", "stop")]) \
        .first()
    df_dim_date = spark.range(start, stop, 24 * 60 * 60) \
        .select(spark_function.col("id").cast("timestamp").cast("date").alias("date")) \
        .withColumn("year", spark_function.year("date")) \
        .withColumn("month", spark_function.month("date")) \
        .withColumn("day", spark_function.dayofmonth("date")) \
        .withColumn("day_of_week", spark_function.dayofweek("date")) \
        .withColumn("day_of_year", spark_function.dayofyear("date"))

    logging.info("write data dimension")
    df_dim_date.write.jdbc(URL, table='dim_date', properties=CREDS, mode='overwrite')


def load_fact_orders_to_dwh(*args, **kwargs) -> None:
    fact = "fact_orders"

    logging.info(f"Load fact: {fact} for data mart: sales")

    logging.info("connected to spark")
    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath', '/home/user/shared/postgresql-42.3.1.jar') \
        .master('local') \
        .appName('sales_to_dwh') \
        .getOrCreate()
    logging.info(f"connected to DWH")
    CONN = BaseHook.get_connection('DWH')
    URL = f"jdbc:postgresql://{CONN.host}:{CONN.port}/{CONN.schema}"
    CREDS = {"user": CONN.login, "password": CONN.password}

    TABLE = "orders"
    DATA_PATH = "/silver/dshop/"
    DATA_PATH = DATA_PATH + TABLE
    logging.info("read data from table to dataframe")
    df_silver_orders = spark.read.parquet(DATA_PATH)

    logging.info("prepeare data facts")
    df_fact_orders = df_silver_orders \
        .select('order_id', 'product_id', 'client_id', 'order_date', 'quantity')
    logging.info("write data facts")
    df_fact_orders.write.jdbc(URL, table='fact_orders', properties=CREDS, mode='overwrite')

    dimension = "dim_clients"
    logging.info(f"read data from dimension DWH")
    df_dim_clients = spark.read.jdbc(URL, table='dim_clients', properties=CREDS)

    logging.info("found missing values for dimension")
    df_missing_dimension_clients = df_fact_orders \
        .join(df_dim_clients, 'client_id', 'left_anti') \
        .groupby('client_id') \
        .count() \
        .withColumn('client_name', spark_function.lit('Unknown')) \
        .select('client_id', 'client_name')

    if df_missing_dimension_clients.count() > 0:
        logging.info("write dimension")
        df_missing_dimension_clients.write.jdbc(URL, table='dim_clients', properties=CREDS, mode='append')

    dimension = "dim_products"
    logging.info("read data dimension from DWH")
    df_dim_products = spark.read.jdbc(URL, table='dim_products', properties=CREDS)

    logging.info("found missing values for dimension: ")
    df_missing_dim_products = df_fact_orders \
        .join(df_dim_products, 'product_id', 'left_anti') \
        .groupby('product_id') \
        .count() \
        .withColumn('product_name', spark_function.lit('Unknown')) \
        .withColumn('aisle', spark_function.lit('Unknown')) \
        .withColumn('department', spark_function.lit('Unknown')) \
        .select('product_id', 'product_name', 'aisle', 'department')

    if df_missing_dim_products.count() > 0:
        logging.info("write dimension")
        df_missing_dim_products.write.jdbc(URL, table='dim_products', properties=CREDS, mode='append')


def load_fact_out_of_stock_to_dwh(*args, **kwargs) -> None:
    fact = "fact_out_of_stock"
    logging.info("connected to spark")
    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath', '/home/user/shared/postgresql-42.3.1.jar') \
        .master('local') \
        .appName('sales_to_dwh') \
        .getOrCreate()

    logging.info("connected DWH")
    CONN = BaseHook.get_connection('DWH')
    URL = f"jdbc:postgresql://{CONN.host}:{CONN.port}/{CONN.schema}"
    CREDS = {"user": CONN.login, "password": CONN.password}

    TABLE = "out_of_stock_app"
    DATA_PATH = "/silver/"
    DATA_PATH = DATA_PATH + TABLE
    logging.info("read data from table to  dataframe")
    df_silver_out_of_stock_app = spark.read.parquet(DATA_PATH)

    logging.info("prepear data for fact ")
    df_fact_out_of_stock = df_silver_out_of_stock_app
    logging.info("write fact")

    df_fact_out_of_stock.write.jdbc(URL, table='fact_out_of_stock', properties=CREDS, mode='overwrite')
    dimension = "dim_products"
    logging.info(f"read data dimension DWH")
    df_dim_products = spark.read.jdbc(URL, table='dim_products', properties=CREDS)
    logging.info(f"found missing values for dimension")
    df_missing_dim_products = df_fact_out_of_stock \
        .join(df_dim_products, 'product_id', 'left_anti') \
        .groupby('product_id') \
        .count() \
        .withColumn('product_name', spark_function.lit('Unknown')) \
        .withColumn('aisle', spark_function.lit('Unknown')) \
        .withColumn('department', spark_function.lit('Unknown')) \
        .select('product_id', 'product_name', 'aisle', 'department')

    if df_missing_dim_products.count() > 0:
        logging.info(f"Begin of write dimension: {dimension}")
        df_missing_dim_products.write.jdbc(URL, table='dim_products', properties=CREDS, mode='append')
