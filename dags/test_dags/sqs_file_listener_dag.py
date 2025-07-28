from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.sqs import SqsHook
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    dag_id="sqs_file_listener_dag_test",
    default_args=default_args,
    start_date=days_ago(1),
    schedule="@continuous",  
    catchup=False,
    max_active_runs=1,
    tags=["sqs", "file", "event-driven"],
) as dag:

    
    wait_for_message = SqsSensor(
        task_id="wait_for_sqs_message",
        aws_conn_id="aws_sqs_pragma", 
        sqs_queue="https://sqs.us-east-2.amazonaws.com/518870435714/QueueEventPragma",  
        max_messages=1,
        num_batches=1,
        wait_time_seconds=20,       
        visibility_timeout=120,    
        delete_message_on_reception=False,  
        deferrable=True,
        poke_interval=5,
        timeout=600,
        soft_fail=True,
    )

    
    def print_file_name_from_message(ti, **_):
        messages = ti.xcom_pull(task_ids="wait_for_sqs_message", key="messages") or []
        if not messages:
            print("❌ No llegaron mensajes.")
            return
        msg = messages[0]
        body = json.loads(msg["Body"])
        file_name = body.get("file_name")
        print(f"✅ Nombre del archivo recibido desde SQS: {file_name}")

        
        hook = SqsHook(aws_conn_id="aws_default")
        client = hook.get_client_type("sqs")
        queue_url = ti.xcom_pull(task_ids="wait_for_sqs_message", key="sqs_queue_url")
        client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=msg["ReceiptHandle"]
        )

    process_message = PythonOperator(
        task_id="print_file_name",
        python_callable=print_file_name_from_message,
        provide_context=True,
        op_kwargs={
        "queue_url": "https://sqs.us-east-2.amazonaws.com/518870435714/QueueEventPragma"
        },
    )

    wait_for_message >> process_message