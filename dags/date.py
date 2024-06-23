from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

# Definisi fungsi untuk membuat tabel dan memasukkan data
def create_and_insert_date_table():
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    connection = hook.get_conn()
    cursor = connection.cursor()

    # Membuat tabel date_dimension
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS date_dimension (
        date_id SERIAL PRIMARY KEY,
        date DATE,
        day INTEGER,
        month INTEGER,
        year INTEGER,
        weekday INTEGER
    );
    """
    
    # Memasukkan data ke dalam tabel date_dimension
    insert_data_sql = """
    INSERT INTO date_dimension (date, day, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s);
    """

    # Menjalankan query untuk membuat tabel
    cursor.execute(create_table_sql)
    
    # Memasukkan data untuk rentang tanggal tertentu
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 12, 31)
    current_date = start_date
    while current_date <= end_date:
        cursor.execute(insert_data_sql, (
            current_date,
            current_date.day,
            current_date.month,
            current_date.year,
            current_date.weekday()
        ))
        current_date += timedelta(days=1)
    
    # Commit perubahan dan menutup koneksi
    connection.commit()
    cursor.close()
    connection.close()

# Definisi default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

# Definisi DAG
dag = DAG(
    "create_date_dimension_table",
    default_args=default_args,
    description="Create Date Dimension Table",
    schedule_interval="@once",
    start_date=datetime(2024, 6, 22),
    catchup=False,
)

# Menambahkan tugas PythonOperator untuk membuat dan mengisi tabel date_dimension
create_date_dimension_task = PythonOperator(
    task_id="create_date_dimension_table",
    python_callable=create_and_insert_date_table,
    dag=dag,
)