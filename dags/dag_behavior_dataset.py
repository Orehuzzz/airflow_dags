from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.hooks.base_hook import BaseHook
from params import task_load_csv_to_db

connection = BaseHook.get_connection('spark_default')

default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2025, 5, 1)
    #"retry_delay": timedelta(minutes=0.1)
}

dag = DAG('dag_spark_behavior', default_args=default_args, schedule_interval='30 12 * * *', catchup=True,
          max_active_tasks=3, max_active_runs=1, tags=["behavior_dag", "spark+apache+sql"])

spark_submit_task = SparkSubmitOperator(
    task_id='submit_spark_job',
    conn_id='spark_default',
    application='/jupyter_notebook_files/spark_exercise.py',
    name='spark_job',
    dag=dag,
)

load_csv_task = PythonOperator(
    task_id='load_csv_to_db',
    python_callable=task_load_csv_to_db,
    dag=dag,
)

spark_submit_task >> load_csv_task
