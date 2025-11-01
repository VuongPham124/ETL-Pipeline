from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

from extract import extract_load_staging
from transform_dim_customer import transform_dim_customers
from transform_dim_products import transform_dim_products
from transform_dim_sellers import transform_dim_sellers
from transform_dim_geolocation import transform_dim_geolocation
from transform_dim_dates import transform_dim_dates
from transform_dim_order_payments import transform_dim_order_payments
from transform_fact_orders import transform_fact_orders

# Cấu hình DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,            # Task không cần đợi lần chạy trước thành công
    'start_date': datetime(2025, 10, 31),
    'email_on_failure':True,
    'email_on_retry':True,
    'retries':0,
    'retry_delay': timedelta(minutes=5),
}

# Tạo DAG chạy hàng ngày
with DAG(
    dag_id='etl_staging_to_dw',
    default_args = default_args,
    description='ETL pipeline: Extract MySQL -> Transform $ Load to PostgreSQL (DW)',
    schedule_interval = timedelta(days=1),          # Chạy mỗi ngày 1 lần
    catchup = False,                                # Không chạy bù các ngày cũ
    tags = ['data_warehouse', 'ecommerce'],         # Thẻ tìm kiếm trong UI
) as dag:

    # Extract
    with TaskGroup(group_id="extract") as extract_group:
        extract_task = PythonOperator(
            task_id = 'extract_load_staging',
            python_callable = extract_load_staging,
        )

    # Transform Dim Table
    with TaskGroup(group_id="transform_dim") as transform_group:
        t_dim_customers = PythonOperator(
            task_id = 'transform_dim_customer',
            python_callable = transform_dim_customers,
        )

        t_dim_products = PythonOperator(
            task_id = 'transform_dim_products',
            python_callable = transform_dim_products,
        )

        t_dim_sellers = PythonOperator(
            task_id = 'transform_dim_sellers',
            python_callable = transform_dim_sellers,
        )

        t_dim_geo = PythonOperator(
            task_id = 'transform_dim_geolocation',
            python_callable = transform_dim_geolocation,
        )

        t_dim_dates = PythonOperator(
            task_id = 'transform_dim_dates',
            python_callable = transform_dim_dates,
        )

        t_dim_payments = PythonOperator(
            task_id = 'transform_dim_payments',
            python_callable = transform_dim_order_payments,
        )

    # Load Fact Table
    with TaskGroup(group_id = "load_fact") as load_group:
        t_fact_orders = PythonOperator(
            task_id = 'transform_fact_orders',
            python_callable = transform_fact_orders,
        )
    
    # Thiết lập thứ tự chạy (dependency)
    extract_group >> transform_group >> load_group