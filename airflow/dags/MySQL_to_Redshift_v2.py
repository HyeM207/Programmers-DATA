from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.models import Variable

from datetime import datetime
from datetime import timedelta

"""
v2_mysql -> s3 -> redshift로 데이터 적재하는 실습 (Incremental Update)
- 사용된 operator 2가지 : SqlToS3Operator, S3ToRedshiftOperator
- 필요한 작업: mysql과 s3에 대한 권한 및 연결 정보를 Airflow Connection에 등록 필요
- 추가된 부분 : 
    - SqlToS3Operator의 query
    - S3ToRedshiftOperator의 method와 upsert_keys
"""

dag = DAG(
    dag_id = 'MySQL_to_Redshift_v2',
    start_date = datetime(2023,1,1), # 날짜가 미래인 경우 실행이 안됨
    schedule = '0 9 * * *',  # 적당히 조절
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

schema = "hmk9667"
table = "nps"
s3_bucket = "grepp-data-engineering"
s3_key = schema + "-" + table       # s3_key = schema + "/" + table

sql = "SELECT * FROM prod.nps WHERE DATE(created_at) = DATE('{{ execution_date }}')"
print(sql)
mysql_to_s3_nps = SqlToS3Operator(
    task_id = 'mysql_to_s3_nps',
    query = sql,
    s3_bucket = s3_bucket,
    s3_key = s3_key,
    sql_conn_id = "mysql_conn_id",
    aws_conn_id = "aws_conn_id",
    verify = False,
    replace = True,
    pd_kwargs={"index": False, "header": False},    
    dag = dag
)

s3_to_redshift_nps = S3ToRedshiftOperator(
    task_id = 's3_to_redshift_nps',
    s3_bucket = s3_bucket,
    s3_key = s3_key,
    schema = schema,
    table = table,
    copy_options=['csv'],
    redshift_conn_id = "redshift_dev_db",
    aws_conn_id = "aws_conn_id",    
    method = "UPSERT",
    upsert_keys = ["id"],
    dag = dag
)

mysql_to_s3_nps >> s3_to_redshift_nps
