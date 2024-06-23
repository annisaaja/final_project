import pandas as pd
import fastavro
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

# Fungsi untuk memuat data order item dari file Avro ke tabel PostgreSQL
def order_item_funnel():
    try:
        # Menginisialisasi hook PostgreSQL dan engine SQLAlchemy
        hook = PostgresHook(postgres_conn_id="postgres_dw")   # Membuat hook PostgreSQL dengan ID koneksi 'postgres_dw'
        engine = hook.get_sqlalchemy_engine()  # Mendapatkan engine SQLAlchemy dari hook PostgreSQL

        # Membuka file Avro dalam mode baca-biner
        with open("data/order_item.avro", 'rb') as f:
            reader = fastavro.reader(f)  # Membaca data dari file Avro
            df = pd.DataFrame(list(reader))  # Mengubah data menjadi DataFrame pandas
            print("Dataframe created successfully")
            print(df.head())  # Print the first few rows of the dataframe

            # Menulis data ke tabel PostgreSQL
            df.to_sql("order_item", engine, if_exists="replace", index=False)
            print("Data order_item berhasil dimasukkan ke dalam tabel 'order_item'.")
    except Exception as e:
        # Tangani error jika terjadi
        print(f"Terjadi kesalahan: {str(e)}")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

dag = DAG(
    "ingest_order_item",
    default_args=default_args,
    description="Order_item Data Ingestion",
    schedule_interval="@once",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

task_load_order_item = PythonOperator(
    task_id="ingest_order_item",
    python_callable=order_item_funnel,
    dag=dag,
)
