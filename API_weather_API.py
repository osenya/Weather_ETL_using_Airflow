from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import requests
import pandas as pd
import duckdb
import json
import os

# Optional backup file path
BACKUP_PATH = "/tmp/weather_raw.json"

# ----------------------------
# Extract Task
# ----------------------------
def extract_weather_data(ti):
    print("Extracting data from OpenWeatherMap API")

    api_key = Variable.get("OWM_API_KEY")
    city = "Nairobi"
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"

    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    ti.xcom_push(key="weather_raw", value=data)

# -------------------------------------------------------------------
# transform task
#----------------------------------------------------------------------

def transform_weather_data(ti):
    print('Transforming data')
    data=ti.xcom_pull(task_id='Extract_task', key="weather_raw")

    # extracting the data
    df= pd.DataFrame(
                         [
                           {
                               "timestamp": datetime.utcnow().isoformat(),
                               "city":data["name"],
                               "temperature":data["main"]["temp"],
                               "humidity":data["main"]["humidity"],
                               "weather":data["weather"][0]["description"]
                           }
                        ]
                    )
    # pushing the transformed data
ti.xcom_push(key="weather_df", value=df.to_dict())

#--------------------------------------------------------------------------------------------
# load data
#--------------------------------------------------------------------------------------------

def load_weather_data(ti):
    print("loading data")
    df_dict = ti.xcom_pull(task_ids = "transform_task", key="weather_df")
    df=pd.DataFrame.from_dict(df_dict)


    # save to duck db--creating a duckdb instance in the folder temp
    conn = duckdb.connect(database ="tmp/weather.duckdb")
    conn.execute('CREATE TABLE IF NOT EXISTS weather AS SELECT * FROM')
    conn.execute("INSERT INTO weather SELECT * FROM df")
    conn.close()
    print("dta  inserted into weather duck db successfully")

#----------------------------------------------------------------------
# DAG definition
#------------------------------------------------------------------------

with DAG (
         dag_id="weather_etl_upgraded",
         start_date= datetime(2025,5,28),
           schedule="@dai;y",
            catchup=False,
            defualt_args=default-args

        ) as dag:
    extract_task = PythonOperator(task_id = "extract_task",
                                  python_callable=extract_weather_data
                                  )
    transform_task = PythonOperator(task_id = "transform_task",
                                    python_callable = transform_weather_data
                                    )
    load_task = PythonOperator(
                                task_id = "load_task",
                                python_callable = load_weather_data
                               )
    extract_task >> transform_task >> load_task



