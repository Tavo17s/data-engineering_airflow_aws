import os
import requests
import pandas as pd


from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator

import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 23),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

city = 'SANTA MARTA'
api_key = ''

def get_forecast(response, i):

    fecha = response['forecast']['forecastday'][0]['hour'][1]['time'].split()[0]
    hora = int(response['forecast']['forecastday'][0]['hour'][i]['time'].split()[1].split(':')[0])
    condicion = response['forecast']['forecastday'][0]['hour'][0]['condition']['text']
    temperatura = response['forecast']['forecastday'][0]['hour'][0]['temp_c']
    rain_prob = response['forecast']['forecastday'][0]['hour'][0]['will_it_rain']
    chance_rain = response['forecast']['forecastday'][0]['hour'][0]['chance_of_rain']

    return fecha, hora, condicion, temperatura, rain_prob, chance_rain

def transform_weather_data(task_instance):

    response = task_instance.xcom_pull(task_ids='extract_weather_data')

    datos = []
    
    for i in range(len(response['forecast']['forecastday'][0]['hour'])):
        datos.append(get_forecast(response, i))

    col = ['fecha', 'hora', 'condicion', 'temperatura', 'rain_prob', 'chance_rain']
    df = pd.DataFrame(datos, columns=col)

    forecast_df = df[['hora', 'condicion', 'chance_rain']]
    forecast_df.set_index('hora', inplace=True)
    
    S3_BUCKET_NAME = 'gustavo-airflow-python'
    CSV_NAME = 'santa_marta-weather'

    now = datetime.now()
    dt_string = now.strftime("-%Y-%m-%d-%H:%M:%S")
    aws_credentials = {"key": "xxxxxxxxx", "secret": "xxxxxxxxxx", "token": "xxxxxxxxxxxxxx"}

    CSV_NAME = CSV_NAME + dt_string
    forecast_df.to_csv(f's3://{S3_BUCKET_NAME}/{CSV_NAME}.csv', index=False, storage_options=aws_credentials)

with DAG('weather_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    is_weather_api_ready = HttpSensor(
        task_id = 'is_weather_api_ready',
        http_conn_id = 'weather_api',
        endpoint = f'v1/forecast.json?key={api_key}&q={city}&days=1&aqi=no&alerts=no'
    )

    extract_weather_data = SimpleHttpOperator(
        task_id = 'extract_weather_data',
        http_conn_id = 'weather_api',
        endpoint = f'v1/forecast.json?key={api_key}&q={city}&days=1&aqi=no&alerts=no',
        method = 'GET',
        response_filter = lambda x: json.loads(x.text),
        log_response = True
    )

    transform_load_data = PythonOperator(
        task_id = 'transform_load_data',
        python_callable = transform_weather_data
    )

    is_weather_api_ready >> extract_weather_data >> transform_load_data

