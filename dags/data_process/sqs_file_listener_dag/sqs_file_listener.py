from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import ShortCircuitOperator

import os

from dags.data_process.sqs_file_listener_dag.utils.general_utils import (get_sqs_client,
                poll_single_sqs_message,
                check_if_file_was_received,
                cleanup_single_message)



# Parámetros configurables
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")




def build_dag():
    with DAG(
        dag_id="sqs_file_listener",
        start_date=days_ago(1),
        schedule_interval="*/2 * * * *",
        catchup=False,
        max_active_runs=1,
        tags=["sensor", "sqs"]
    ) as dag:

        poll_sqs = PythonOperator(
            task_id="poll_sqs",
            python_callable=poll_single_sqs_message,
        )
        
        check_for_messages = ShortCircuitOperator(
            task_id='check_for_messages',
            python_callable=check_if_file_was_received,
            provide_context=True
        )

        trigger_process_dag = TriggerDagRunOperator(
            task_id="trigger_processing_dag",
            trigger_dag_id="process_data_transfer_file_pragma",
            conf={"file_name": "{{ ti.xcom_pull(task_ids='poll_sqs', key='file_name') }}"},
            wait_for_completion=False,
            reset_dag_run=False
        )

        cleanup = PythonOperator(
            task_id="cleanup_message",
            python_callable=cleanup_single_message,
            trigger_rule=TriggerRule.ALL_DONE
        )

        poll_sqs >> check_for_messages >> trigger_process_dag >> cleanup

    return dag

dag = build_dag()