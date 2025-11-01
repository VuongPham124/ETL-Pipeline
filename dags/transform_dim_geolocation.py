import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

def transform_dim_geolocation():
    staging = PostgresHook(postgres_conn_id='postgres')
    warehouse = PostgresHook(postgres_conn_id='postgres')

    # Đọc data từ staging
    df = staging.get_pandas_df("SELECT * FROM staging.stg_geolocation")
    if df.empty:
        print("Không có dữ liệu trong bảng staging.stg_geolocation")
        return
    
    # Làm sạch dữ liệu
    df['geolocation_zip_code_prefix'] = df['geolocation_zip_code_prefix'].astype(str).str.zfill(5)
    df['geolocation_city'] = df['geolocation_city'].str.title().str.strip()
    df['geolocation_state'] = df['geolocation_state'].str.upper().str.strip()

    # Loại bỏ trùng lặp
    df = df.drop_duplicates(subset=['geolocation_zip_code_prefix'])

    # Tạo surrogate key
    df['geolocation_key'] = df.index + 1

    # Lưu vào schema warehouse
    engine = warehouse.get_sqlalchemy_engine()
    df.to_sql(
        name='dim_geolocation',
        con=engine,
        schema='warehouse',
        if_exists='replace',
        index=False
    )
    print("Hoàn thành transform và lưu dữ liệu vào warehouse.dim_geolocation")