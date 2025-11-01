import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

def extract_load_staging(**kwargs):
    source = MySqlHook(mysql_conn_id='mysql')
    staging = PostgresHook(postgres_conn_id='postgres')

    # Tạo schema staging nếu chưa có
    staging.run("CREATE SCHEMA IF NOT EXISTS staging;")
    staging.run("CREATE SCHEMA IF NOT EXISTS warehouse;")
    
    tables = [
        "product_category_name_translation",
        "geolocation",
        "sellers",
        "customers",
        "products",
        "orders",
        "order_items",
        "order_payments",
        "order_reviews"
    ]

    for table in tables:
        # Đọc dữ liệu từ MySQL
        df = source.get_pandas_df(sql=f"SELECT * FROM {table}")
        print(f"DEBUG: Bảng {table} → {len(df)} dòng")

        # Bỏ qua nếu không có dữ liệu
        if df is None or df.empty:
            print(f"Bảng {table} rỗng → bỏ qua")
            continue

        # Tên bảng trong staging
        stg_table_name = f"stg_{table}"

        # Ghi vào PostgreSQL
        engine = staging.get_sqlalchemy_engine()
        df.to_sql(
            name=stg_table_name,
            con=engine,
            schema='staging',
            if_exists='replace',
            index=False,
            chunksize=5000
        )
        print(f"Đã lưu: {table} → {stg_table_name}")