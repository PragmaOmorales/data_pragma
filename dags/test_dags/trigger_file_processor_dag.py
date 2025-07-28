from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 7, 28),
    "catchup": False,
}

with DAG(
    dag_id="trigger_file_processor_dag",
    description="DAG that triggers another DAG and sends file_name",
    schedule_interval=None,
    default_args=default_args,
    tags=["trigger", "s3"],
) as dag:

    def get_target_file(**kwargs):
        # Simula que obtienes un archivo dinámicamente, puedes leer desde SQS, DB, etc.
        file_name = "2012-1.csv"
        kwargs["ti"].xcom_push(key="file_name", value=file_name)

    fetch_file_task = PythonOperator(
        task_id="fetch_file_to_process",
        python_callable=get_target_file,
    )

    trigger_process_dag = TriggerDagRunOperator(
    task_id="trigger_example_processor",
    trigger_dag_id="process_data_transfer_file_pragma",
    conf={"file_name": "2012-3.csv"},
    )

    fetch_file_task >> trigger_process_dag