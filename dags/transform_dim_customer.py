import pandas as pd
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook

def transform_dim_customers():
    staging = PostgresHook(postgres_conn_id='postgres')
    warehouse = PostgresHook(postgres_conn_id='postgres')

    # Đọc dữ liệu từ staging
    df = staging.get_pandas_df("SELECT * FROM staging.stg_customers")

    if df.empty:
        print("Không có dữ liệu trong bảng staging.stg_customers")
        return
    
    # Làm sạch dữ liệu
    df['customer_unique_id'] = df['customer_unique_id'].astype(str)
    df['customer_zip_code_prefix'] = df['customer_zip_code_prefix'].astype(str).str.zfill(5)
    df['customer_city'] = df['customer_city'].str.title().str.strip()
    df['customer_state'] = df['customer_state'].str.upper().str.strip()

    # Loại bỏ trùng lặp, giữ bản ghi mới nhất
    df = (
        df.sort_values(by='customer_id')
          .drop_duplicates(subset='customer_unique_id', keep='last')
          .reset_index(drop=True)
    )

    # Tạo surrogate key
    df['customer_key'] = df.index + 1

    # Thiết lập thời gian hiệu lực (SCD Type 2)
    current_date = datetime.now().date()
    future_date = current_date + timedelta(days=365*10)
    df['effective_date'] = current_date
    df['end_date'] = future_date
    df['is_current'] = True

    # Ghi vào warehouse
    engine = warehouse.get_sqlalchemy_engine()
    df.to_sql(
        name='dim_customers',
        con=engine,
        schema='warehouse',
        if_exists='replace',
        index=False,
        chunksize=5000
    )

    print("Hoàn thành transform và lưu dữ liệu vào warehouse.dim_customers")