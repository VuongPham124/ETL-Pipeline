import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

def transform_dim_sellers():
    staging = PostgresHook(postgres_conn_id='postgres')
    warehouse = PostgresHook(postgres_conn_id='postgres')

    # Đọc data từ staging
    df = staging.get_pandas_df("SELECT * FROM staging.stg_sellers")
    if df.empty:
        print("Không có dữ liệu trong bảng staging.stg_sellers")
        return
    
    # Làm sạch dữ liệu
    df['seller_zip_code_prefix'] = df['seller_zip_code_prefix'].astype(str).str.zfill(5)
    df['seller_city'] = df['seller_city'].str.title().str.strip()
    df['seller_state'] = df['seller_state'].str.upper().str.strip()

    # Loại bỏ trùng lặp
    df = df.drop_duplicates(subset=['seller_id'])

    # Tạo surrogate key
    df['seller_key'] = df.index + 1

    # Thêm cột để theo dõi thay đổi (SCD Type 1)
    df['last_updated'] = pd.Timestamp.now().date()

    # Lưu vào schema
    engine = warehouse.get_sqlalchemy_engine()
    df.to_sql(
        name='dim_sellers',
        con=engine,
        schema='warehouse',
        if_exists='replace',
        index=False
    )

    print("Hoàn thành transform và lưu dữ liệu vào warehouse.dim_sellers")