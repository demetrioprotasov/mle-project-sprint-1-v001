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

def prepare_dataset():
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
            'real_estate_raw',
            metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('flat_id', Integer),
            Column('building_id', Integer),
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
            UniqueConstraint('flat_id', name='unique_flat_id_constraint')
        )
        if not inspect(db_conn).has_table(real_estate.name): 
            metadata.create_all(db_conn)        

    @task()
    def extract(**kwargs):

        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        sql = f"""
        select
            b.id as building_id, f.id as flat_id, 
            b.build_year, b.building_type_int, b.latitude, b.longitude, b.ceiling_height, b.flats_count, b.floors_total, b.has_elevator,
            f.floor, f.kitchen_area, f.living_area, f.rooms, f.is_apartment, f.studio, f.total_area, f.price 
        from buildings as b
        inner join flats as f on b.id = f.building_id
        """
        data = pd.read_sql(sql, conn)
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):
        for col in ['has_elevator', 'is_apartment', 'studio']:
            data[col] = data[col].astype(int)
        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table="real_estate_raw",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['flat_id'],
            rows=data.values.tolist()
        )
        
    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)

prepare_dataset()
