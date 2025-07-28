from airflow.decorators import dag, task
from datetime import datetime

@task
def hello_world():
    print("Hello World")

@dag(
    dag_id='hello_world',
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    default_args={'owner': 'airflow', 'retries': 1},
)
def hello_world_dag():
    hello_world_task = hello_world()

hello_world_dag()