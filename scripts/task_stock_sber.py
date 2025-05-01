from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, VARCHAR, Date,  Float,  TIME
from sqlalchemy.orm import declarative_base
import argparse


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

class Weather(Base):
    __tablename__ = 'behavior'
    customer_id = Column(Integer, nullable=False, unique=True, primary_key=True, autoincrement=True)
    gender = Column(VARCHAR)
    age = Column(Integer)
    city = Column(VARCHAR)
    membership_type = Column(VARCHAR)
    total_spend = Column(Float)
    items_purchased = Column(Integer)
    average_rating = Column(Float)
    discount_applied = Column(Float)
    satisfaction_level = Column(VARCHAR)

