from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base_hook import BaseHook

connection = BaseHook.get_connection('main_postgresql_connection')

default_args = {
    "owner": "etl_user",
    "depends_on_past": False, #определяем зависит ли выполнение каждой таски от успешного выполнения предыдущей
    "start_date": datetime(2025, 2, 18),
    #"retry_delay": timedelta(minutes=0.1)
}

dag = DAG('dag_currency', default_args=default_args, schedule_interval='30 9 * * *', catchup=True,
          max_active_tasks=3, max_active_runs=1, tags=["currency_dah", "first test dag"])

task1 = BashOperator(
    task_id='task_currency',
    bash_command='python3 /root/airflow/scripts/task_currency.py',
    dag=dag)

task2 = BashOperator(
    task_id='task2',
    bash_command='python3 /root/airflow/scripts/task_plug.py',
    dag=dag)
