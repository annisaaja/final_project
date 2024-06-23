from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

def create_sales_table():
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    connection = hook.get_conn()
    cursor = connection.cursor()
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS sales AS
    SELECT 
        o.id AS order_id,
        o.customer_id,
        oi.product_id,
        oi.coupon_id,
        dd.date_id,
        oi.amount,
        SUM(oi.amount * p.price) AS total_price
    FROM 
        orders o
    JOIN 
        order_item oi ON o.id = oi.order_id
    JOIN 
        product p ON oi.product_id = p.id
    JOIN 
        date_dimension dd ON o.created_at::date = dd.date
    GROUP BY 
        o.id, o.customer_id, oi.product_id, oi.coupon_id, dd.date_id, oi.amount;
    """
    
    cursor.execute(create_table_sql)
    connection.commit()
    cursor.close()
    connection.close()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

dag = DAG(
    "create_sales",
    default_args=default_args,
    description="Create Sales Table",
    schedule_interval="@once",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

create_order_facts_task = PythonOperator(
    task_id="create_sales",
    python_callable=create_sales_table,
    dag=dag,
)
