import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

def transform_dim_order_payments():
    staging = PostgresHook(postgres_conn_id='postgres')
    warehouse = PostgresHook(postgres_conn_id='postgres')

    # Đọc data từ staging
    df = staging.get_pandas_df("SELECT * FROM staging.stg_order_payments")
    if df.empty:
        print("Không có dữ liệu trong bảng staging.stg_order_payments")
        return
    
    # Làm sạch dữ liệu
    df['payment_type'] = df['payment_type'].str.lower()
    df['payment_installments'] = df['payment_installments'].fillna(1).astype(int)
    df['payment_description'] = df['payment_type'] + '_x' + df['payment_installments'].astype(str)

    # Loại bỏ trùng
    df = df.drop_duplicates(subset=['payment_type', 'payment_installments'])

    # Tạo surrogate key
    df['payments_key'] = df.index + 1

    # Lưu vào schema
    engine = warehouse.get_sqlalchemy_engine()
    df.to_sql(
        name='dim_order_payments',
        con=engine,
        schema='warehouse',
        if_exists='replace',
        index=False
    )

    print("Hoàn thành transform và lưu dữ liệu vào warehouse.dim_order_payments")