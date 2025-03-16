from datetime import datetime
from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 26),
    #"retry_delay": timedelta(minutes=0.1)
}

dag = DAG('data_showcase_dag', default_args=default_args, schedule_interval='0 * * * *', catchup=True,
          max_active_tasks=3, max_active_runs=1, tags=["showcase_dag", "dm_orders"])

clear_day = PostgresOperator(
    task_id='clear_day',
    postgres_conn_id='main_postgresql_connection',
    sql="""DELETE FROM public.dm_orders WHERE "buy_date" = '{{ ds }}'::date""",
    dag=dag)


task_main = PostgresOperator(
    task_id='main_task',
    postgres_conn_id='main_postgresql_connection',
    sql="""INSERT INTO dm_orders(buy_date, product_name, city_name, value)
SELECT buy_time::DATE AS buy_date, dp.product_name, dc.city_name ,  SUM(value) AS value
FROM f_orders ord
LEFT JOIN d_cities dc 
ON dc.id = ord.city_id 
LEFT JOIN d_products dp 
ON ord.product_id = dp.id 
GROUP BY buy_date, product_name, city_name;""",
    dag=dag
)

clear_day >> task_main