from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

from datetime import datetime
from datetime import timedelta

import requests
import logging
import json

"""
v1_OpenWeather API를 이용해 오늘로부터 향후 8일의 시간별 날씨 정보 가져옴
- Full Refresh와 INSERT INTO 이용함
"""

def get_Redshift_connection():
    # autocommit 디폴트는 False
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()

@task
def etl(schema, table):
    api_key = Variable.get("open_weather_api_key")
    # 서울의 위도/경도
    lat = 37.5665
    lon = 126.9780

    url = f"https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&appid={api_key}&units=metric&exclude=current,minutely,hourly,alerts"
    response = requests.get(url)
    data = json.loads(response.text) # response.json()
    ret = []
    print("check data : ", data)
    
    # daily에는 앞으로 8일간 날씨 정보 들어감
    for d in data["daily"]:
        day = datetime.fromtimestamp(d["dt"]).strftime('%Y-%m-%d') # epoch의 형식 변환
        ret.append("('{}',{},{},{})".format(day, d["temp"]["day"], d["temp"]["min"], d["temp"]["max"]))

    cur = get_Redshift_connection()
    drop_recreate_sql = f"""DROP TABLE IF EXISTS {schema}.{table};
CREATE TABLE {schema}.{table} (
    date date,
    temp float,
    min_temp float,
    max_temp float,
    created_date timestamp default GETDATE()
);
"""
    insert_sql = f"""INSERT INTO {schema}.{table} VALUES """ + ",".join(ret) # 한 번에 insert함
    logging.info(drop_recreate_sql)
    logging.info(insert_sql)
    # 예외처리 (raise 잊지말기)
    try:
        cur.execute(drop_recreate_sql)
        cur.execute(insert_sql)
        cur.execute("Commit;")
    except Exception as e:
        cur.execute("Rollback;")
        raise

with DAG(
    dag_id = 'Weather_to_Redshift',
    start_date = datetime(2023,5,30), 
    schedule = '0 2 * * *',  
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:

    etl("hmk9667", "weather_forecast")
