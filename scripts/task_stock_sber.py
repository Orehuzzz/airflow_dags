from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, VARCHAR, Date, Boolean, Float, TIMESTAMP, text, TIME
from sqlalchemy.orm import declarative_base
import argparse
import requests
import datetime

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

Base = declarative_base()


class StocksSber(Base):
    __tablename__ = 'sber_stocks'
    id = Column(Integer, nullable=False, unique=True, primary_key=True, autoincrement=True)
    name = Column(VARCHAR)
    time = Column(TIME)
    date = Column(Date)
    open_price = Column(Float)
    low_price = Column(Float)
    high_price = Column(Float)
    last_price = Column(Float)
    usd_price = Column(Float)


engine = create_engine(SQLALCHEMY_DATABASE_URI)


class RequestSender:
    def __init__(self):
        self.data_json = None

    def get_SBER_stocks(self):
        self.URL = f'https://iss.moex.com/iss/engines/stock/markets/shares/securities/SBER.json'
        try:
            response = requests.get(url=self.URL)
            response.raise_for_status()
            self.data_json = response.json()
        except:
            print('API request Error')



stocks = RequestSender()
stocks.get_SBER_stocks()

columns = stocks.__dict__['data_json']['marketdata']['columns']
values = stocks.__dict__['data_json']['marketdata']['data'][0]

name_stocks = values[0]
last_price_index = columns.index("LAST")
open_price_index = columns.index('OPEN')
usd_price_index = columns.index('VALUE_USD')
time_index = columns.index('TIME')
low_price_index = columns.index('LOW')
high_price_index = columns.index('HIGH')

name = name_stocks
last_price = values[last_price_index]
open_price = values[open_price_index]
usd_price = values[usd_price_index]
time = values[time_index]
date = datetime.date.today().isoformat()
low_price = values[low_price_index]
high_price = values[high_price_index]


Base.metadata.create_all(bind=engine)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
session_local = SessionLocal()

record = StocksSber(name=name,
                    time=time,
                    date=date,
                    open_price=open_price,
                    low_price=low_price,
                    high_price=high_price,
                    last_price=last_price,
                    usd_price=usd_price
    )

session_local.add(record)
session_local.commit()