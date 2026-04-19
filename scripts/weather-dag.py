import pandas as pd
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
    return round(temp_in_fahrenheit, 3)

def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="group_a.extract_weather_data")
    
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temperature_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_fahrenheit= kelvin_to_fahrenheit(data["main"]["feels_like"])
    minimum_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    maximum_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = {"city": city,
                        "description": weather_description,
                        "temperature_fahrenheit": temperature_fahrenheit,
                        "feels_like_fahrenheit": feels_like_fahrenheit,
                        "minimum_temp_fahrenheit": minimum_temp_fahrenheit,
                        "maximum_temp_fahrenheit": maximum_temp_fahrenheit,
                        "pressure": pressure,
                        "humidity": humidity,
                        "wind_speed": wind_speed,
                        "time_of_record": time_of_record,
                        "sunrise_local_time":sunrise_time,
                        "sunset_local_time": sunset_time                        
                        }
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)
    
    df_data.to_csv("boston_weather_data.csv", index=False, header=False)


def load_weather():
    hook= PostgresHook(postgres_conn_id= 'postgres_conn')
    hook.copy_expert(
        sql= "COPY boston_weather_data FROM stdin WITH DELIMITER as ','",
        filename= 'boston_weather_data.csv'
    )


def save_in_s3(task_instance):
    data = task_instance.xcom_pull(task_ids="join_data")
    df = pd.DataFrame(data, columns = ['city', 'description', 'temperature_fahrenheit', 'feels_like_fahrenheit', 'minimun_temp_fahrenheit', 
    'maximum_temp_fahrenheit', 'pressure','humidity', 'wind_speed', 'time_of_record', 'sunrise_local_time', 'sunset_local_time',
    'census_2020', 'census_est_2024', 'population_change'])
    # df.to_csv('city_pop_and_weather.csv')
    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'joined_weather_data_' + dt_string
    df.to_csv(f"s3://weather-api-parallel-processing-etl/{dt_string}.csv", index=False)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 18),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}


with DAG('weather_dag_2',
        default_args=default_args,
        schedule = '@daily',
        catchup=False) as dag:

        start_pipeline = EmptyOperator(
            task_id = 'start_pipeline'
        )

        join_data= SQLExecuteQueryOperator(
            task_id= 'join_data',
            conn_id= 'postgres_conn',
            sql= '''
            SELECT w.city, description, temperature_farenheit as temperature_fahrenheit, feels_like_farenheit as feels_like_fahrenheit,
            minimun_temp_farenheit as minimum_temp_fahrenheit, maximum_temp_farenheit as maximum_temp_fahrenheit, pressure,
            humidity, wind_speed, time_of_record, sunrise_local_time, sunset_local_time,
            c.census_2020, c.census_est_2024,
            (c.census_est_2024 - c.census_2020) as population_change
            FROM api_weather_data w
            INNER JOIN my_city_look_up c
                ON w.city= c.city
            ;
            '''
        )

        load_joined_data=  PythonOperator(
            task_id= 'load_joined_data',
            python_callable= save_in_s3
        )

        end_pipeline= EmptyOperator(
            task_id = 'end_pipeline'
        )


        with TaskGroup(group_id = 'group_a', tooltip= "Extract_from_S3_and_weatherapi") as group_A:
            
            create_table_1 = SQLExecuteQueryOperator(
                task_id='create_table_1',
                conn_id = "postgres_conn",
                sql= '''  
                    CREATE TABLE IF NOT EXISTS my_city_look_up (
                    city TEXT NOT NULL,
                    census_2020 numeric NOT NULL,
                    census_est_2024 numeric NOT NULL                    
                );
                '''
            )

            truncate_table = SQLExecuteQueryOperator(
                task_id='truncate_table',
                conn_id = "postgres_conn",
                sql= ''' TRUNCATE TABLE my_city_look_up;
                    '''
            )

            uploadS3_to_postgres  = SQLExecuteQueryOperator(
                task_id = "uploadS3_to_postgres",
                conn_id = "postgres_conn",
                sql = "SELECT aws_s3.table_import_from_s3('my_city_look_up', '', '(format csv, DELIMITER '','', HEADER true)', 'weather-api-parallel-processing-etl', 'City-Population.csv', 'us-east-1');"
            )

            create_table_2 = SQLExecuteQueryOperator(
                task_id='create_table_2',
                conn_id = "postgres_conn",
                sql= ''' 
                    CREATE TABLE IF NOT EXISTS boston_weather_data (
                    city TEXT,
                    description TEXT,
                    temperature_fahrenheit NUMERIC,
                    feels_like_fahrenheit NUMERIC,
                    minimum_temp_fahrenheit NUMERIC,
                    maximum_temp_fahrenheit NUMERIC,
                    pressure NUMERIC,
                    humidity NUMERIC,
                    wind_speed NUMERIC,
                    time_of_record TIMESTAMP,
                    sunrise_local_time TIMESTAMP,
                    sunset_local_time TIMESTAMP                    
                );
                '''
            )

            is_boston_weather_api_ready = HttpSensor(
                task_id ='is_boston_weather_api_ready',
                http_conn_id='weathermap_api',
                endpoint='/data/2.5/weather?q=boston&APPID=4f07d0a645b873239c2232412827fa50'
            )

            extract_weather_data= HttpOperator(
                task_id= 'extract_weather_data',
                http_conn_id= 'weathermap_api',
                endpoint= '/data/2.5/weather?q=boston&APPID=4f07d0a645b873239c2232412827fa50',
                method= 'GET',
                response_filter= lambda r: json.loads(r.text),
                log_response= True
            )

            
            transform_load_weather_data= PythonOperator(
                task_id= 'transform_load_weather_data',
                python_callable= transform_load_data
            )

            load_weather_data= PythonOperator(
                task_id= 'load_weather_data',
                python_callable= load_weather
            )


            create_table_1 >> truncate_table >> uploadS3_to_postgres
            create_table_2 >> is_boston_weather_api_ready >> extract_weather_data >> transform_load_weather_data >> load_weather_data
        start_pipeline >> group_A >> join_data >> load_joined_data >> end_pipeline
