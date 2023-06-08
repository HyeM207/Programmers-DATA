from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

"""
기초 실습_BashOperator 사용해보기
"""

default_args = {
   'owner': 'hyemin',
   'start_date': datetime(2023, 5, 27, hour=0, minute=00),
   'email': ['hmk9667@gmail.com'],
   'retries': 1,
   'retry_delay': timedelta(minutes=3),
}

test_dag = DAG(
   "dag_v1",
   schedule="0 9 * * *", 
   tags=['test'],
   catchup=False,
   default_args=default_args 
)

t1 = BashOperator(
   task_id='print_date',
   bash_command='date',
   dag=test_dag)

t2 = BashOperator(
   task_id='sleep',
   bash_command='sleep 5',
   dag=test_dag)

t3 = BashOperator(
   task_id='ls',
   bash_command='ls /tmp',
   dag=test_dag)

t1 >> [ t2, t3 ]