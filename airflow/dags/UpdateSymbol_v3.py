from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from pandas import Timestamp

import yfinance as yf
import logging


"""
Day4 숙제_UpdateSymbol_v2의 Incremental Update 방식 수정해보기
- UpdateSymbol_v2 : Yahoo Finance API로 애플 주식 수집/파싱/적재 실습
- 변경사항: 
    - created_date 추가
    - ROW_NUMBER() 추가하여 중복제거
"""

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def get_historical_prices(symbol):
    """
    Extract&Transform
    """
    ticket = yf.Ticker(symbol)
    data = ticket.history()
    records = []

    for index, row in data.iterrows():
        date = index.strftime('%Y-%m-%d %H:%M:%S')
        records.append([date, row["Open"], row["High"], row["Low"], row["Close"], row["Volume"]])

    return records


def _create_table(cur, schema, table, drop_first):
    """
    테이블 생성
    """
    if drop_first:
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        date date,
        "open" float,
        high float,
        low float,
        close float,
        volume bigint,
        created_date timestamp default GETDATE()
    );""")


@task
def load(schema, table, records):
    logging.info("load started")
    cur = get_Redshift_connection()
    try:
        # 트랜잭션 (Incremental Update 형태)
        cur.execute("BEGIN;")
        # 원본 테이블이 없으면 생성 
        _create_table(cur, schema, table, False)
        # 임시 테이블에 원본 테이블의 레코드 복사
        cur.execute(f"CREATE TEMP TABLE t AS SELECT * FROM {schema}.{table};")
        for r in records:
            sql = f"INSERT INTO t VALUES ('{r[0]}', {r[1]}, {r[2]}, {r[3]}, {r[4]}, {r[5]});"
            print(sql)
            cur.execute(sql)

        # 원본 테이블에 임시 테이블 내용 복사 (+중복제거)
        alter_sql = f"""DELETE FROM {schema}.{table};
        INSERT INTO {schema}.{table}
            SELECT date, "open", high, low, close, volume FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY created_date DESC) seq
                FROM t
            )
            WHERE seq = 1;"""
        cur.execute(alter_sql) 
        cur.execute("COMMIT;") 
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;") 
        raise
    logging.info("load done")


with DAG(
    dag_id = 'UpdateSymbol_v3',
    start_date = datetime(2023,5,30),
    catchup=False,
    tags=['API'],
    schedule = '0 10 * * *'
) as dag:

    results = get_historical_prices("AAPL")
    load("hmk9667", "stock_info_v3", results)
