from datetime import datetime
from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 26),
    #"retry_delay": timedelta(minutes=0.1)
}

dag = DAG('stocks_mart_dag', default_args=default_args, schedule_interval='0 * * * *', catchup=True,
          max_active_tasks=3, max_active_runs=1, tags=["mart_dag", 'sber_airoflot_stocks'])

clear_day = PostgresOperator(
    task_id='clear_day',
    postgres_conn_id='main_postgresql_connection',
    sql="""DELETE FROM data_mart.mart_stocks WHERE "date" = '{{ ds }}'::date""",
    dag=dag)

#Объединяем две таблицы с акциями в одну
insert_task = PostgresOperator(
    task_id='insert_task',
    postgres_conn_id='main_postgresql_connection',
    sql="""INSERT INTO data_mart.f_mart_stocks (id, name, time, date, open_price, low_price, high_price, last_price,
    usd_price)
    SELECT id, name, time, date, open_price, low_price, high_price, last_price,
    usd_price
    FROM public.sber_stocks
    UNION 
    SELECT id, name, time, date, open_price, low_price, high_price, last_price,
    usd_price
    FROM public.airoloflot_stocks""",
    dag=dag
)

join_task = PostgresOperator(
    task_id='join_task',
    postgress_conn_id='main_postgresql_connection',
    sql="""SELECT id, name, time, date, open_price, low_price, high_price, last_price,
    usd_price
    FROM data_mart.f_mart_stocks
    """
)
