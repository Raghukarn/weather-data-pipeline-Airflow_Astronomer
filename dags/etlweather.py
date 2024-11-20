from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook #to load data from API
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import requests
import json

#  Latitude, Longitude for locaion (here we are taking London for example)
LATITUDE = '33.0198' # '51.5074'
LONGITUDE = '96.6989' # '-0.1278'

POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

default_args = {
    'owner':'airflow',
    'start_date':days_ago(1)
}

# DAG

with DAG(
    dag_id='weather_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
         ) as dags:
    
    @task()
    def extract_weather_data():
        """Extract weather data from Open-Meteo API uisng Airflow Connection"""

        # Use Http Hook to get the connection details from Airflow Connection
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')

        #  Build API endpoitns
        endpoint = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'

        # Make the request via HTTP hook
        response = http_hook.run(endpoint)

        if response.status_code ==200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")
        
    @task
    def transfrom_weather_data(weather_data):
        """Transform the extracted weather data"""

        current_weather = weather_data['current_weather']
        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode']

        }
        return transformed_data
    
    @task
    def load_weather_data(transformed_data):
        """Load the transformed data into PostgreSQL"""
        pg_hook = PostgresHook(postgres_conn_id = POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table if not exists
        cursor.execute("""
                    CREATE TABLE IF NOT EXISTS weather_data (
                       latitude FLOAT,
                       longitude FLOAT,
                       temperature FLOAT,
                       windspeed FLOAT,
                       winddirection FLOAT,
                       weathercode INT,
                       timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    """)
        #  INSERT THE TRANSFORMED DATA INTO THE TBALE
        cursor.execute("""
                    INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
                      VALUES (%s, %s, %s, %s, %s, %s)
                      """, (
                          transformed_data['latitude'],
                          transformed_data['longitude'],
                          transformed_data['temperature'],
                          transformed_data['windspeed'],
                          transformed_data['winddirection'],
                          transformed_data['weathercode']
                      ))
        conn.commit()
        cursor.close()

    #  DAG workflow - ETL pipeline
    weather_data = extract_weather_data()
    transformed_data = transfrom_weather_data(weather_data)
    load_weather_data(transformed_data)

        
