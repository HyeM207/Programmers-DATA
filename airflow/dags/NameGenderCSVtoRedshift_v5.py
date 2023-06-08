from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

from datetime import datetime
from datetime import timedelta

import requests
import logging

"""
v5_ @task 이용하여 태스크 지정 
"""

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def extract(url):
    logging.info(datetime.utcnow())
    f = requests.get(url)
    return f.text


@task
def transform(text):
    lines = text.strip().split("\n")[1:] 
    records = []
    for l in lines:
        (name, gender) = l.split(",") 
        records.append([name, gender])
    logging.info("Transform ended")
    return records


@task
def load(schema, table, records):
    logging.info("load started")    
    cur = get_Redshift_connection()   
    # 트랜잭션 (Full Refresh 형태)
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
    logging.info("load done")


with DAG(
    dag_id='namegender_v5',
    start_date=datetime(2022, 10, 6), 
    schedule='0 2 * * *',
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        # 'on_failure_callback': slack.on_failure_callback,
    }
) as dag:

    url = Variable.get("csv_url")
    schema = 'hmk9667'   
    table = 'name_gender'

    lines = transform(extract(url))
    load(schema, table, lines)