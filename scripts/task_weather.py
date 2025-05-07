from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, Float
from sqlalchemy.orm import declarative_base
from params.global_params import API_WEATHER #скрываем API-ключ
import argparse
import requests

#Парсим данные для подключения к БД
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

#Подлючение к БД
SQLALCHEMY_DATABASE_URI = f"postgresql://{str(v_user)}:{str(v_password)}@{str(v_host)}:{str(v_port)}/{str(v_dbname)}"

Base = declarative_base()

class Weather(Base):
    __tablename__ = 'weather'
    id = Column(Integer, nullable=False, unique=True, primary_key=True, autoincrement=True)
    temp = Column(Float)
    feels_like = Column(Float)
    temp_min = Column(Float)
    temp_max = Column(Float)
    pressure = Column(Float)
    humidity = Column(Float)
    sea_level = Column(Float)
    grnd_level = Column(Float)


engine = create_engine(SQLALCHEMY_DATABASE_URI)


class RequestSender:
    def get_weather_by_city(self, city_name: str):
        self.URL = f'https://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={str(API_WEATHER)}&units=metric'
        try:
            self.r = requests.get(url=self.URL)
            self.r_json = self.r.json()
        except:
            print('API request Error')


request_sender = RequestSender()
request_sender.get_weather_by_city('Moscow')


temp = request_sender.__dict__['r_json']['main'].get('temp', None)
feels_like = request_sender.__dict__['r_json']['main'].get('feels_like', None)
temp_min = request_sender.__dict__['r_json']['main'].get('temp_min', None)
temp_max = request_sender.__dict__['r_json']['main'].get('temp_max', None)
pressure = request_sender.__dict__['r_json']['main'].get('pressure', None)
humidity = request_sender.__dict__['r_json']['main'].get('humidity', None)
sea_level = request_sender.__dict__['r_json']['main'].get('sea_level', None)
grnd_level = request_sender.__dict__['r_json']['main'].get('grnd_level', None)


Base.metadata.create_all(bind=engine)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
session_local = SessionLocal()

new_record = Weather(
                    temp=temp,
                    feels_like=feels_like,
                    temp_min=temp_min,
                    pressure=pressure,
                    humidity=humidity,
                    sea_level=sea_level,
                    grnd_level=grnd_level
                    )

#При добавлении второго + параметра
# new_record_two = Weather(
#                     temp=temp,
#                     feels_like=feels_like,
#                     temp_min=temp_min,
#                     pressure=pressure,
#                     humidity=humidity,
#                     sea_level=sea_level,
#                     grnd_level=grnd_level
#                     )



session_local.add(new_record)
# session_local.add(new_record_two) При добавлении второго параметра

session_local.commit()
