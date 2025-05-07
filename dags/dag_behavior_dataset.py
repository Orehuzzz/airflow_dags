# Попытка в спарк
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine
from params import parametrs
import pandas as pd
import glob


connection = BaseHook.get_connection('spark_default')

default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2025, 5, 1)
    # "retry_delay": timedelta(minutes=0.1)
}

dag = DAG('dag_spark_behavior', default_args=default_args, schedule_interval='@daily', catchup=False,
          tags=["behavior_dag", "spark+apache+sql"])

spark_submit_task = SparkSubmitOperator(
    task_id='submit_spark_job',
    application='/jupyter_notebook_files/spark_exercise.py',  # Путь к скрипту
    conn_id='spark_default',  # Имя подключения в Airflow
    name='spark_job',  # Имя приложения в Spark UI
    verbose=True,
    conf={
        "spark.master": "spark://vm3190178.stark-industries.solutions:7077",
        "spark.submit.deployMode": "client"  # Явно указываем режим
    },
    spark_binary='/root/spark/bin/spark-submit',  # Полный путь к spark-submit
    dag=dag
)


def load_csv_to_db():
    # Загрузка CSV в DataFrame
    csv_files = glob.glob('/jupyter_notebook_files/data/clear/part-00000-*.csv')


    latest_csv = sorted(csv_files)[-1]  # Берем самый новый

    engine = BaseHook.get_connection('main_postgresql_connection')

    df = pd.read_csv(latest_csv)

    df.to_sql(name='public.spark_table',
              con=engine,
              if_exists='append',
              index=False)


load_csv_task = PythonOperator(
    task_id='load_csv_to_db',
    python_callable=load_csv_to_db,
    dag=dag,
)

spark_submit_task >> load_csv_task