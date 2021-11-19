from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from Engine.spark_db_to_bronze import load_postgres_to_bronze
from Engine.spark_bronze_to_silver import (load_db_bronze_to_silver,
                                           load_bronze_dsshop_api_orders_to_silver)
from Engine.spark_silver_to_dwh import *

with DAG(
        dag_id='postgres_data_pipeline'
        , start_date=datetime(2021, 11, 19, 11)
        , schedule_interval='@daily'
) as dag:
    load_bronze_aisles = PythonOperator(
        task_id=f'load_to_bronze_table_aisles',
        dag=dag,
        python_callable=load_postgres_to_bronze,
        provide_context=True,
        op_kwargs={'table': 'aisles', 'postgres_conn_id': 'dshop'},
    )

    load_bronze_clients = PythonOperator(
        task_id=f'load_to_bronze_table_clients',
        dag=dag,
        python_callable=load_postgres_to_bronze,
        provide_context=True,
        op_kwargs={'table': 'clients', 'postgres_conn_id': 'dshop'},
    )

    load_bronze_departments = PythonOperator(
        task_id=f'load_to_bronze_table_departments',
        dag=dag,
        python_callable=load_postgres_to_bronze,
        provide_context=True,
        op_kwargs={'table': 'departments', 'postgres_conn_id': 'dshop'},
    )

    load_bronze_orders = PythonOperator(
        task_id=f'load_to_bronze_table_orders',
        dag=dag,
        python_callable=load_postgres_to_bronze,
        provide_context=True,
        op_kwargs={'table': 'orders', 'postgres_conn_id': 'dshop'},
    )

    load_bronze_products = PythonOperator(
        task_id=f'load_to_bronze_table_products',
        dag=dag,
        python_callable=load_postgres_to_bronze,
        provide_context=True,
        op_kwargs={'table': 'products', 'postgres_conn_id': 'dshop'},
    )

    load_silver_aisles = PythonOperator(
        task_id=f'load_to_silver_table_aisles',
        dag=dag,
        python_callable=load_db_bronze_to_silver,
        provide_context=True,
        op_kwargs={'table': 'aisles', 'project': 'dshop'},
    )

    load_silver_clients = PythonOperator(
        task_id=f'load_to_silver_table_clients',
        dag=dag,
        python_callable=load_db_bronze_to_silver,
        provide_context=True,
        op_kwargs={'table': 'clients', 'project': 'dshop'},
    )

    load_silver_departments = PythonOperator(
        task_id=f'load_to_silver_table_departments',
        dag=dag,
        python_callable=load_db_bronze_to_silver,
        provide_context=True,
        op_kwargs={'table': 'departments', 'project': 'dshop'},
    )

    load_silver_products = PythonOperator(
        task_id=f'load_to_silver_table_products',
        dag=dag,
        python_callable=load_db_bronze_to_silver,
        provide_context=True,
        op_kwargs={'table': 'products', 'project': 'dshop'},
    )

    load_silver_orders = PythonOperator(
        task_id=f'load_to_silver_table_orders',
        dag=dag,
        python_callable=load_bronze_dsshop_api_orders_to_silver,
        provide_context=True,
    )

    load_dwh_dimension_clients = PythonOperator(
        task_id=f'load_to_dwh_dimension_clients',
        dag=dag,
        python_callable=load_dim_clients_to_dwh,
        provide_context=True,
    )

    load_to_dwh_dimension_products = PythonOperator(
        task_id=f'load_to_dwh_dimension_products',
        dag=dag,
        python_callable=load_dimmension_to_dwh,
        provide_context=True,
    )

    load_to_dwh_dimension_date = PythonOperator(
        task_id=f'load_to_dwh_dimension_date',
        dag=dag,
        python_callable=load_dimension_date_to_dwh,
        provide_context=True,
    )

    load_to_dwh_fact_orders = PythonOperator(
        task_id=f'load_to_dwh_fact_orders',
        dag=dag,
        python_callable=load_fact_orders_to_dwh,
        provide_context=True,
    )

    load_to_dwh_fact_out_of_stock = PythonOperator(
        task_id=f'load_to_dwh_fact_out_of_stock',
        dag=dag,
        python_callable=load_fact_out_of_stock_to_dwh,
        provide_context=True,
    )

    load_bronze_aisles >> load_silver_aisles >> load_dwh_dimension_clients >> load_to_dwh_fact_orders

    load_bronze_clients >> load_silver_clients >> load_dwh_dimension_clients
    
    load_bronze_departments >> load_silver_departments >> load_dwh_dimension_clients

    load_bronze_products >> load_silver_products >> load_to_dwh_dimension_products

    load_bronze_orders >> load_silver_orders >> load_to_dwh_fact_orders

    load_to_dwh_dimension_date >> load_to_dwh_fact_orders
    load_to_dwh_dimension_date >> load_to_dwh_fact_out_of_stock

    load_to_dwh_dimension_products >> load_to_dwh_fact_orders
    load_to_dwh_dimension_products >> load_to_dwh_fact_out_of_stock
