from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
from datetime import timedelta

import requests
import logging
import json

"""
v2_OpenWeather API를 이용해 오늘로부터 향후 8일의 시간별 날씨 정보 가져옴
- Incremental Update와 ROW_NUMBER를 이용한 중복 제거
"""


def get_Redshift_connection():
    # autocommit 디폴트는 False
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()


@task
def etl(schema, table, lat, lon, api_key):

    # Extract
    url = f"https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&appid={api_key}&units=metric&exclude=current,minutely,hourly,alerts"
    response = requests.get(url)
    data = json.loads(response.text)

    """
    {'dt': 1622948400, 'sunrise': 1622923873, 'sunset': 1622976631, 'moonrise': 1622915520, 'moonset': 1622962620, 'moon_phase': 0.87, 'temp': {'day': 26.59, 'min': 15.67, 'max': 28.11, 'night': 22.68, 'eve': 26.29, 'morn': 15.67}, 'feels_like': {'day': 26.59, 'night': 22.2, 'eve': 26.29, 'morn': 15.36}, 'pressure': 1003, 'humidity': 30, 'dew_point': 7.56, 'wind_speed': 4.05, 'wind_deg': 250, 'wind_gust': 9.2, 'weather': [{'id': 802, 'main': 'Clouds', 'description': 'scattered clouds', 'icon': '03d'}], 'clouds': 44, 'pop': 0, 'uvi': 3}
    """
    # Transform
    ret = []
    for d in data["daily"]:
        day = datetime.fromtimestamp(d["dt"]).strftime('%Y-%m-%d')
        ret.append("('{}',{},{},{})".format(day, d["temp"]["day"], d["temp"]["min"], d["temp"]["max"]))

    # Load
    cur = get_Redshift_connection()
    
    # 1) 원본 테이블이 없다면 생성
    create_table_sql = f"""CREATE TABLE IF NOT EXISTS {schema}.{table} (
    date date,
    temp float,
    min_temp float,
    max_temp float,
    created_date timestamp default GETDATE()
);"""
    logging.info(create_table_sql)

    # 2) 임시 테이블 생성
    create_t_sql = f"""CREATE TEMP TABLE t AS SELECT * FROM {schema}.{table};"""
    logging.info(create_t_sql)
    try:
        cur.execute(create_table_sql)
        cur.execute(create_t_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

    # 3) 임시 테이블 데이터 입력
    # ret를 저장할 때 '('로 묶어주어 한 번에 join 가능함
    insert_sql = f"INSERT INTO t VALUES " + ",".join(ret)
    logging.info(insert_sql)
    try:
        cur.execute(insert_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

    # 4) 기존 테이블 대체
    alter_sql = f"""DELETE FROM {schema}.{table};
        INSERT INTO {schema}.{table}
        SELECT date, temp, min_temp, max_temp FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY created_date DESC) seq
        FROM t
        )
        WHERE seq = 1;"""
    logging.info(alter_sql)
    try:
        cur.execute(alter_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise


with DAG(
    dag_id = 'Weather_to_Redshift_v2',
    start_date = datetime(2022,8,24), 
    schedule = '0 4 * * *',  
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:

    etl("hmk9667", "weather_forecast_v2", 37.5665, 126.9780, Variable.get("open_weather_api_key"))
