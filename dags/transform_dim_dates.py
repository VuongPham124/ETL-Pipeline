import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

def transform_dim_dates():
    warehouse = PostgresHook(postgres_conn_id='postgres')

    # Tạo bảng dim_dates
    start = pd.Timestamp('2016-01-01')
    end = pd.Timestamp('2025-12-31')
    date_range = pd.date_range(start=start, end=end)

    df = pd.DataFrame({
        'date_key': date_range,
        'day': date_range.day,
        'month': date_range.month,
        'year': date_range.year,
        'quarter': date_range.quarter,
        'day_of_week': date_range.dayofweek,
        'day_name': date_range.strftime('%A'),
        'month_name': date_range.strftime('%B'),
        'is_weekend': date_range.dayofweek.isin([5,6])
    })

    engine = warehouse.get_sqlalchemy_engine()
    df.to_sql(
        name='dim_dates',
        con=engine,
        schema='warehouse',
        if_exists='replace',
        index=False
    )

    print("Hoàn thành transform và lưu dữ liệu vào warehouse.dim_dates")