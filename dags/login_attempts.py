from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import pyarrow.parquet as pq
#import fastavro
import json
import requests

#define the data directory
data_dir = '/opt/airflow/data'


def load_login_attempts():
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()
    all_login_attempts = []
    for i in range(10):
        with open(f'{data_dir}/login_attempts_{i}.json') as f:
            data = json.load(f)
            all_login_attempts.extend(data)
    login_attempts = pd.DataFrame(all_login_attempts)
    login_attempts.to_sql('login_attempts', con=engine, if_exists='replace', index=False)

# Define the DAG
dag = DAG(
    "ingest_company_data",
    #default_args=default_args,
    description="ingest data into PostgreSQL",
    schedule_interval="@once",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)


t1 = PythonOperator(
    task_id='load_login_attempts',
    python_callable=load_login_attempts,
    dag=dag,
)

# Set task dependencies
t1