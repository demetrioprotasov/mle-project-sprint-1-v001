from airflow.decorators import dag, task
import pendulum
from telegram_notifier import send_telegram_success_message, send_telegram_failure_message

@dag(
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL"],
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message
)

def clean_dataset():
    import pandas as pd
    import numpy as np
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    @task()
    def create_table():
        from sqlalchemy import MetaData, Table, Column, Integer, BigInteger, Float, UniqueConstraint
        from sqlalchemy import inspect
        
        hook = PostgresHook('destination_db')
        db_conn = hook.get_sqlalchemy_engine()
        metadata = MetaData()
        real_estate = Table(
            'real_estate_clean',
            metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('flat_id', Integer),
            Column('build_year', Integer),
            Column('building_type_int', Integer),
            Column('latitude', Float),
            Column('longitude', Float),
            Column('ceiling_height', Float),
            Column('flats_count', Integer),
            Column('floors_total', Integer),
            Column('has_elevator', Integer),
            Column('floor', Integer),
            Column('kitchen_area', Float),
            Column('living_area', Float),
            Column('rooms', Integer),
            Column('is_apartment', Integer),
            Column('studio', Integer),
            Column('total_area', Float),
            Column('price', BigInteger),
            UniqueConstraint('flat_id', name='unique_flat_id_clean_constraint')
        )
        if not inspect(db_conn).has_table(real_estate.name): 
            metadata.create_all(db_conn)        

    @task()
    def extract(**kwargs):

        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        sql = f"""
        select *
        from real_estate_raw
        """
        data = pd.read_sql(sql, conn)
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):
        def remove_duplicates(data):
            feature_cols = data.columns.drop('flat_id').tolist()
            is_duplicated_features = data.duplicated(subset=feature_cols, keep=False)
            data = data[~is_duplicated_features].reset_index(drop=True)
            return data

        def remove_outliers(data):
            # Явные ошибки и опечатки
            data = data[data['ceiling_height'].between(2.0, 5.0)]      # реальные потолки
            data = data[data['price'].between(100_000, 1_000_000_000)] # 100К–1млрд ₽
            data = data[data['total_area'].between(10, 500)]           # 10–500 м²
            data = data[data['living_area'] <= 300]                    # до 300 м²
            data = data[data['kitchen_area'] <= 100]                   # до 100 м²
            data = data[data['rooms'] <= 10]                           # до 10 комнат
            data = data[data['floors_total'].between(1, 100)]          # 1–100 этажей
            return data.reset_index(drop=True)
 
        def fill_missing_values(data):
            num_features = ['build_year', 'latitude', 'longitude', 'ceiling_height', 'flats_count', 'floors_total', 'floor', 'kitchen_area', 'living_area', 'rooms', 'total_area']
            cat_features = ['building_type_int', 'has_elevator', 'is_apartment', 'studio']
            for col in num_features:
                fill_value = data[col].mean()
                data[col] = data[col].fillna(fill_value)
            for col in cat_features:
                fill_value = data[col].mode().iloc[0]
                data[col] = data[col].fillna(fill_value)
            return data 
        
        data = data.drop(columns=['id', 'building_id'])
        data = remove_duplicates(data)
        data = remove_outliers(data)
        data = fill_missing_values(data)
        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table="real_estate_clean",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['flat_id'],
            rows=data.values.tolist()
        )
        
    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)

clean_dataset()
