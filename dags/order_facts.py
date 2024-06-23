from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

def create_order_facts_table():
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    connection = hook.get_conn()
    cursor = connection.cursor()
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS order_facts AS
    SELECT 
        o.id AS order_id,
        o.customer_id,
        o.created_at AS order_date,
        o.status,
        SUM(oi.amount) AS total_amount,
        COUNT(oi.product_id) AS total_items,
        COALESCE(c.discount_percent, 0) AS total_discount
    FROM 
        orders o
    JOIN 
        order_item oi ON o.id = oi.order_id
    LEFT JOIN 
        coupons c ON oi.coupon_id = c.id
    GROUP BY 
        o.id, o.customer_id, o.created_at, o.status, c.discount_percent;
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
    "create_order_facts",
    default_args=default_args,
    description="Create Order Facts Table",
    schedule_interval="@once",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

create_order_facts_task = PythonOperator(
    task_id="create_order_facts",
    python_callable=create_order_facts_table,
    dag=dag,
)