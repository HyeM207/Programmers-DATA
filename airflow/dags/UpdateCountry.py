from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
from datetime import timedelta

import logging
import requests
import json


"""
Day3숙제_세계 나라 정보 API 사용하여 DAG작성하기
- Full Refresh로 구현
- @task, Connections 이용
- DAG : 매주 토요일 오전 6시 30분에 실행됨
"""

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def extract(url):
    """
    URL에서 JSON데이터 가져와 파싱
    """
    logging.info(datetime.utcnow())
    response = requests.get(url)
    data = json.loads(response.text)
    return data


@task
def transform(data):
    """
    필요한 데이터만 추출
    """
    records = []
    for d in data:
        country = d["name"]["official"]
        population = d["population"]
        area = d["area"] 
        records.append([country, population, area])
    logging.info("transform ended")
    return records


@task
def load(schema, table, records):
    """
    Redshift에 테이블 생성하여 데이터 삽입
    """
    logging.info("load started")
    cur = get_Redshift_connection()
    try:
        # 트랜잭션 (Full Refresh 형태)
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"""
        CREATE TABLE {schema}.{table} (
            country varchar(100),
            population bigint,
            area float
        );""")
        for record in records:
            # 따옴표 처리를 위해 파라미터 바인딩 사용
            sql = f"INSERT INTO {schema}.{table} VALUES (%s, %s, %s);"
            cur.execute(sql, record)
        cur.execute("COMMIT;")   
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise
    logging.info("load done")


default_args = {
    'owner': 'hyemin',
    'email': ['hmk9667@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id = 'UpdateCountry',
    start_date = datetime(2023, 6, 7),
    catchup=False,
    tags=['API'],
    schedule = '30 6 * * 6', # 매주 토요일 오전 6시 30분
    default_args=default_args
) as dag:
    
    records = transform(extract("https://restcountries.com/v3/all"))
    load("hmk9667", "country_info", records)