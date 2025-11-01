import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

def transform_dim_products():
    staging = PostgresHook(postgres_conn_id='postgres')
    warehouse = PostgresHook(postgres_conn_id='postgres')

    # Đọc data từ staging
    df_product = staging.get_pandas_df("SELECT * FROM staging.stg_products")
    df_categories = staging.get_pandas_df("SELECT * FROM staging.stg_product_category_name_translation")

    if df_product.empty and df_categories.empty:
        print("Không có dữ liệu trong bảng staging.stg_products và stg_product_category_name_translation")
        return
    
    # Kết hợp dữ liệu sản phẩm và danh mục
    df = pd.merge(df_product, df_categories, on='product_category_name', how='left')

    # Làm sạch dữ liệu
    df['product_category_name_english'] = df['product_category_name_english'].fillna('Unknown')
    df['product_weight_g'] = df['product_weight_g'].fillna(0)
    df['product_length_cm'] = df['product_length_cm'].fillna(0)
    df['product_height_cm'] = df['product_height_cm'].fillna(0)
    df['product_width_cm'] = df['product_width_cm'].fillna(0)

    # Tạo surrogate key
    df['product_key'] = df.index + 1

    # Thêm cột để theo dõi thay đổi (SCD Type 1)
    df['last_updated'] = pd.Timestamp.now().date()
    
    # Lưu vào schema
    engine = warehouse.get_sqlalchemy_engine()
    df.to_sql(
        name='dim_products',
        con=engine,
        schema='warehouse',
        if_exists='replace',
        index=False
    )

    print("Hoàn thành transform và lưu dữ liệu vào warehouse.dim_products")