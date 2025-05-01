from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, VARCHAR, Date, Boolean, Float, TIMESTAMP, text, String
from sqlalchemy.orm import declarative_base
import argparse
import requests

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

Base = declarative_base()

class Currency(Base):
    __tablename__ = 'currency'
    id = Column(Integer, nullable=False, unique=True, primary_key=True, autoincrement=True)
    base = Column(String)
    date = Column(TIMESTAMP)
    rates_rub = Column(Float)
    rates_eur = Column(Float)


url = 'https://v6.exchangerate-api.com/v6/78ff706ef30e2b15bba6c038/latest/USD'

# Making our request
response = requests.get(url)
data = response.json()


base = data.get('base_code')
date = data.get('time_last_update_utc')
rates_rub = data.get('conversion_rates').get('RUB')
rates_eur = data.get('conversion_rates').get('EUR')


engine = create_engine(SQLALCHEMY_DATABASE_URI)

Base.metadata.create_all(bind=engine)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
session_local = SessionLocal()

new_record = Currency(
                    base=base,
                    date=date,
                    rates_rub=rates_rub,
                    rates_eur=rates_eur
                    )

session_local.add(new_record)
session_local.commit()