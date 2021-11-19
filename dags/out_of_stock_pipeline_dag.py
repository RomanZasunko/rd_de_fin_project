import os
from datetime import datetime
from airflow import DAG
from Engine.http_to_hdfs_operator import HttpToHDFSOperator
from Engine.spark_bronze_json_to_silver import load_data_from_bronze_to_silver
from airflow.operators.python_operator import PythonOperator


def download_data_from_http(ds, **kwargs):
    HttpToHDFSOperator(
        config_path=os.path.join(os.getcwd(), 'airflow', 'dags', 'config', 'config.yaml'),
        app_name='out_of_stock_app',
        date=ds,
        timeout=5,
        hdfs_conn_id='webhdfs',
        hdfs_path=os.path.join('/', 'bronze')
    ).execute()


with DAG(
        dag_id='pipeline_out_of_stock'
        , description='dag for download data from outstock to silver'
        , start_date=datetime(2021, 4, 1, 9)
        , end_date=datetime(2021, 4, 15, 9)
        , schedule_interval='@daily'
) as dag:
    download_data = PythonOperator(
        task_id='get_data_from_http_to_bronze',
        dag=dag,
        provide_context=True,
        python_callable=download_data_from_http
    )

    load_to_silver = PythonOperator(
        task_id=f'load_to_silver_out_of_api',
        dag=dag,
        python_callable=load_data_from_bronze_to_silver,
        provide_context=True,
        op_kwargs={'project': 'out_of_stock_app'},
    )

    download_data >> load_to_silver
