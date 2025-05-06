from datetime import datetime
from airflow.models import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.bash import BashOperator
from utils.check_table_sensor import CheckTableSensor
from airflow.models.connection import Connection

connection = BaseHook.get_connection('main_postgresql_connection')
default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 25),
}

dag = DAG('dag_sensor', default_args=default_args, schedule_interval='30 9 * * *', catchup=True,
          max_active_tasks=3, max_active_runs=1, tags=["weather_sensor_dag", "first sensor dag"])

task1 = BashOperator(
    task_id='task_weather',
    bash_command='python3 /root/airflow/scripts/task_plug.py --date {{ ds }} ' +f'--host {connection.host} --dbname {connection.schema} --user {connection.login} --jdbc_password {connection.password} --port 5432',
    dag=dag)

task_sensor = CheckTableSensor(
    task_id=f'task_check_table_sensor',
    timeout=1000,
    mode='reschedule',
    poke_interval=10,
    conn=connection,
    table_name='weather',
    dag=dag)

task2 = BashOperator(
    task_id='task2',
    bash_command='python3 /root/airflow/scripts/task_weather.py --date {{ ds }} ' +f'--host {connection.host} --dbname {connection.schema} --user {connection.login} --jdbc_password {connection.password} --port 5432',
    dag=dag)

for i in [1, 2, 3, 4, 5]:
    some_task = BashOperator(
    task_id=f'task4_{str(i)}',
    bash_command='python3 /root/airflow/scripts/task_plug.py --date {{ ds }} ' +f'--host {connection.host} --dbname {connection.schema} --user {connection.login} --jdbc_password {connection.password} --port 5432',
    dag=dag)



task1 >> task_sensor >> task2 >> some_task