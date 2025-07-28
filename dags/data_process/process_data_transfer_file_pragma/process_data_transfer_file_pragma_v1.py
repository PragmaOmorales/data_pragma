"""
SUMMARY.

--------
* DAG Name:
    process_data_transfer_file_pragma
* Owner:
    Oscar Andres Morales P.

* Description:
    This DAG monitors a designated folder for new CSV files containing transaction data. Once a file is detected, it is ingested and each transaction 
    is inserted into a PostgreSQL database. The DAG maintains stateful metrics—such as total row count, average price, minimum, and maximum price—without 
    reprocessing historical data. It supports event-driven execution, simulates Slack-based logging, and includes fault tolerance by tracking rejected 
    records and processed file hashes to prevent duplication.
"""

import os
import shutil
from datetime import datetime
from airflow.sensors.python import PythonSensor
from airflow.operators.python import BranchPythonOperator
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from dags.data_process.process_data_transfer_file_pragma.utils.general_utils import (
    file_arrived, decide_processing_path, process_transaction_file, get_latest_file, 
    print_stats_state ,print_slack_trx_state, print_slack_total_trx_state
)

params_definition = {


'queries_base_path': os.path.join(os.path.dirname(__file__), 'queries'),
'input_folder': 'dags/data_process/process_data_transfer_file_pragma/S3',
'postgres_conn_id': 'pg_conn_id'


}

@dag(
    max_active_runs=5 ,
    schedule=None,
    start_date=datetime(2025, 7, 1),
    description='Ingests CSV files from a monitored folder, processes transactions into a PostgreSQL database, and updates stateful statistics incrementally.',
    template_searchpath=params_definition['queries_base_path'],
    tags=["event-driven", "microbatch", "prueba", "pragma"],
    catchup=False,
    concurrency=1,
    doc_md=__doc__,
    
)

def process_data_transfer_file_pragma_v1():
    """DAG Definition."""
  

    
    wait_local_file = PythonSensor(
        task_id='wait_local_file',
        python_callable=lambda: file_arrived(params_definition['input_folder']),
        poke_interval=10,
        timeout=120,
        mode='poke'
    )

    get_latest_file_task = PythonOperator(
        task_id='get_latest_file',
        python_callable=lambda **kwargs: kwargs['ti'].xcom_push(
            key='selected_file',
            value=get_latest_file(params_definition['input_folder'])
        ),
        provide_context=True
    )

    slack_stats_notification = PythonOperator(
        task_id='slack_notification_task',
        python_callable=print_stats_state,
        op_kwargs={
        'postgres_conn_id': params_definition['postgres_conn_id']
        }
    )

    slack_trx_notification = PythonOperator(
        task_id='slack_trx_notification_task',
        python_callable=print_slack_trx_state,
        op_kwargs={
        'postgres_conn_id': params_definition['postgres_conn_id'],
        'file_name': "{{ ti.xcom_pull(task_ids='get_latest_file', key='selected_file') }}"
        }
    )

    slack_total_stats_notification = PythonOperator(
        task_id='slack_total_notification_task',
        python_callable=print_slack_total_trx_state,
        op_kwargs={
        'postgres_conn_id': params_definition['postgres_conn_id']
        }
    )

    

    choose_processing_path = BranchPythonOperator(
    task_id='choose_processing_path',
    python_callable=decide_processing_path,
)
    
    process_file = PythonOperator(
    task_id='process_standard_file',
    python_callable=process_transaction_file,
    op_kwargs={
        'postgres_conn_id': params_definition['postgres_conn_id'],
        'file_name': "{{ ti.xcom_pull(task_ids='get_latest_file', key='selected_file') }}",
        'file_path': params_definition['input_folder']
    }
    ) 


        # Ramas de validación según tipo de archivo
    validate_standard_file = EmptyOperator(
        task_id="validate_standard_file"
    )

    validate_validation_file = EmptyOperator(
        task_id="validate_validation_file"
    )

    # Join final
    end = EmptyOperator(task_id="end")
    

    wait_local_file >> get_latest_file_task >> process_file >> slack_stats_notification >> slack_trx_notification
    slack_trx_notification >> choose_processing_path >> [validate_standard_file, validate_validation_file]
    validate_standard_file >> end
    validate_validation_file >> slack_total_stats_notification >> end

dag = process_data_transfer_file_pragma_v1()