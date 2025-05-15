# Мой пет-проект
![pSQL](https://img.shields.io/badge/psql-16.8-blue)
![Python Version](https://img.shields.io/badge/python-3.8-green)
![Apache Airflow](https://img.shields.io/badge/apache_airflow-2.9.3-orange)
![Spark](https://img.shields.io/badge/spark-3.5.5-yellow)

## 📚 Описание проекта

Данный проект представляет из себя набор DAG-ов, которые используют приведённые скрипты для ETL-процессов.

# 👳‍♂️☝️ Даги

dag_currency - отправляет в БД информацию о курсе доллара к рублю и евро

dag_sber_and_airoflot_stocks - отправляет данные о стоимости акций Сбера и Аэрофлота 

dag_sensor - тестовый даг для пробы сенсоров

dag_weather - отправляет данные о погоде в Москве 

data_showcase_dag - даг для витрины данных - берёт данные из двух таблиц с акциями сбера и аэрофлота и джоинит к ним данные о валюте


# 💻 Технологический стек
```
Python - версия 3.8
```
```
SQLalchemy - для создания БД при необходимости
```
```
Requests - для get-запросов на сайты
```
```
Spark + Pyspark
```
```
Apache Airflow и Postgresql подняты на арендованом сервере
```
```
URL для Apache: http://171.22.117.31:8080
```
# 👀 Данные для Postgresql
```
Хост: 171.22.117.31
```
```
Порт: 5432
```
```
База данных: main_database
```
```
Пользователь: guest
```
```
Пароль: 1ExgPksOwn4K
```

# 🤠 Данные для Apache Airflow
```
Хост: 171.22.117.31
```
```
Порт: 8080
```
```
Пользователь: readonly_user2
```
```
Пароль: 1ExgPksOwn4K
```

