import pandas as pd
import argparse
from sqlalchemy import create_engine


def load_csv_to_db():
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

    SQLALCHEMY_DATABASE_URI = f"postgresql://{v_user}:{v_password}@{v_host}:{v_port}/{v_dbname}"

        # Загрузка CSV в DataFrame
    df = pd.read_csv('/jupyter_notebook_files/data/clear/part-00000-ea3365ac-0859-425a-a100-115b517ee248-c000.csv')

        # Создание подключения к базе данных (PostgreSQL в примере)
    engine = create_engine(SQLALCHEMY_DATABASE_URI)

    df.to_sql('public.spark_table', engine, index=False,
                  if_exists='append')

