from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base_hook import BaseHook
connection = BaseHook.get_connection('main_postgresql_connection')


default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 23),
    #"retry_delay": timedelta(minutes=0.1)
}

dag = DAG('sber_stock_dag', default_args=default_args, schedule_interval='0 21 * * *', catchup=True,
          max_active_tasks=3, max_active_runs=1, tags=["Stocks dag", "test stocks dag"])

task1 = BashOperator(
    task_id='task_stock_sber',
    bash_command='python3 /root/airflow/scripts/task_stock_sber.py --date {{ ds }} ' +f'--host {connection.host} --dbname {connection.schema} --user {connection.login} --jdbc_password {connection.password} --port 5432',
    dag=dag)

task2 = BashOperator(
    task_id='task_stock_airoflot',
    bash_command='python3 /root/airflow/scripts/task_stock_airoflot.py --date {{ ds }} ' +f'--host {connection.host} --dbname {connection.schema} --user {connection.login} --jdbc_password {connection.password} --port 5432',
    dag=dag)

