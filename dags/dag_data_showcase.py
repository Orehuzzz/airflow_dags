from datetime import datetime
from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base_hook import BaseHook

connection = BaseHook.get_connection('main_postgresql_connection')

default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2025, 5, 2),
    #"retry_delay": timedelta(minutes=0.1)
}

dag = DAG('dag_data_showcase', default_args=default_args, schedule_interval='* * * * *', catchup=True,
          max_active_tasks=3, max_active_runs=1, tags=["mart_dag", 'sber_airoflot_stocks'])

#Необходимо удалять пердыдущие данные, чтобы не было аномалий
clear_day = PostgresOperator(
    task_id='clear_day',
    postgres_conn_id='main_postgresql_connection',
    sql="""DELETE FROM publci.showcase_stocks WHERE "date" = '{{ ds }}'::date""",
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

#Join-им колону с валютой - конечная табличка: showcase_stocks
join_task = PostgresOperator(
    task_id='join_task',
    postgres_conn_id='main_postgresql_connection',
    sql="""INSERT INTO showcase_stocks (name, date, open_price, low_price, high_price, last_price, rates_rub, rates_eur)
    SELECT m_s.name, m_s.date, m_s.open_price, m_s.low_price, m_s.high_price, m_s.last_price, c.rates_rub, c.rates_eur
    FROM data_mart.f_mart_stocks m_s
    JOIN public.currency c
    ON m_s.date = c.date;
    """
)

clear_day >> insert_task >> join_task
