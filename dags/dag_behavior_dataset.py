#Попытка в спарк
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine
from params import parametrs
import pandas as pd

connection = BaseHook.get_connection('spark_default')

default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2025, 5, 1)
    #"retry_delay": timedelta(minutes=0.1)
}

dag = DAG('dag_spark_behavior', default_args=default_args, schedule_interval='@daily', catchup=False,
          tags=["behavior_dag", "spark+apache+sql"])

spark_submit_task = SparkSubmitOperator(
    task_id='submit_spark_job',
    application='/jupyter_notebook_files/spark_exercise.py',
    conn_id='spark_default',
    name='spark_job',
    verbose=True,
    dag=dag
)


def load_csv_to_db():

# Загрузка CSV в DataFrame
    conn = create_engine(parametrs.SQLALCHEMY_DATABASE_URI)

    df = pd.read_csv('/jupyter_notebook_files/data/clear/part-00000-ea3365ac-0859-425a-a100-115b517ee248-c000.csv')


    df.to_sql('public.spark_table', conn, index=False,
          if_exists='append')

load_csv_task = PythonOperator(
    task_id='load_csv_to_db',
    python_callable=load_csv_to_db,
    dag=dag,
)

spark_submit_task >> load_csv_task
