from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from datetime import datetime
from datetime import timedelta
import requests
import logging
import psycopg2


"""
v2_ PythonOperator의 params를 통해 변수 넘기기
"""

def get_Redshift_connection():
    host = "learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = "hmk9667" 
    redshift_pass = "..."  
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect(f"dbname={dbname} user={redshift_user} host={host} password={redshift_pass} port={port}")
    conn.set_session(autocommit=True)
    return conn.cursor()


def extract(url):
    logging.info("Extract started")
    f = requests.get(url)
    logging.info("Extract done")
    return (f.text)


def transform(text):
    logging.info("Transform started")	
    lines = text.strip().split("\n")[1:]
    records = []
    for l in lines:
        (name, gender) = l.split(",") 
        records.append([name, gender])
    logging.info("Transform ended")
    return records


def load(records):
    logging.info("load started")
    schema = "hyemin"
    # 트랜잭션 (Full Refresh 형태)
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {schema}.name_gender;") 
        for r in records:
            name = r[0]
            gender = r[1]
            print(name, "-", gender)
            sql = f"INSERT INTO {schema}.name_gender VALUES ('{name}', '{gender}')"
            cur.execute(sql)
        cur.execute("END;")  
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")   
        raise error
    logging.info("load done")

def etl(**context):
    link = context["params"]["url"] # 넘겨받은 csv url
    task_instance = context['task_instance']
    execution_date = context['execution_date']

    logging.info(execution_date)

    data = extract(link)
    lines = transform(data)
    load(lines)


dag = DAG(
    dag_id = 'name_gender_v2',
    start_date = datetime(2023,4,6),
    schedule = '0 2 * * *', 
    catchup = False,
    max_active_runs = 1,	
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)


task = PythonOperator(
    task_id = 'perform_etl',
    python_callable = etl,
    # 넘겨줄 함수 파라미터 설정
    params = {
        'url': "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
    },
    dag = dag)
