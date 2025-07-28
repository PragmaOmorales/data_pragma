from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_received_file_name(**kwargs):
    file_name = kwargs["dag_run"].conf.get("file_name", "⚠️ not provided")
    print(f"📁 Received file name from trigger: {file_name}")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 7, 28),
    "catchup": False,
}

with DAG(
    dag_id="example_file_processor_dag",
    schedule_interval=None,
    default_args=default_args,
    description="Triggered DAG that prints the file name received via conf",
    tags=["example", "trigger"],
) as dag:

    print_file_param_task = PythonOperator(
        task_id="print_file_name",
        python_callable=print_received_file_name,
        provide_context=True,
    )