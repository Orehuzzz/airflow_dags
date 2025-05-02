#Незаконченная попытка в спарк
from datetime import datetime
import argparse
from sqlalchemy import create_engine
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument("--date", dest="date")
parser.add_argument("--host", dest="host")
parser.add_argument("--dbname", dest="dbname")
parser.add_argument("--user", dest="user")
parser.add_argument("--jdbc_password", dest="jdbc_password")
parser.add_argument("--port", dest="port")
args = parser.parse_args()

print('date = ' + str(args.date))
print('host = ' + str(args.host))
print('dbname = ' + str(args.dbname))
print('user = ' + str(args.user))
print('jdbc_password = ' + str(args.jdbc_password))
print('port = ' + str(args.port))

v_host = str(args.host)
v_dbname = str(args.dbname)
v_user = str(args.user)
v_password = str(args.jdbc_password)
v_port = str(args.port)

SQLALCHEMY_DATABASE_URI = f"postgresql://{str(v_user)}:{str(v_password)}@{str(v_host)}:{str(v_port)}/{str(v_dbname)}"

def load_csv_to_db():
    # Загрузка CSV в DataFrame
    df = pd.read_csv('/jupyter_notebook_files/data/clear/part-00000-ea3365ac-0859-425a-a100-115b517ee248-c000.csv')

    # Создание подключения к базе данных (PostgreSQL в примере)
    engine = create_engine(SQLALCHEMY_DATABASE_URI)


    df.to_sql('', engine, index=False,
              if_exists='replace')


# Определение DAG

default_args = {
    "owner": "etl_user",
    "depends_on_past": False, #узнать что значит параметр
    "start_date": datetime(2025, 2, 18),
    #"retry_delay": timedelta(minutes=0.1)
}

dag = DAG('spark_behavior', default_args=default_args, schedule_interval='30 12 * * *', catchup=True,
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
    python_callable=load_csv_to_db,
    dag=dag,
)

spark_submit_task >> load_csv_task
