import os

from airflow.hooks.base import BaseHook
import boto3
import json

QUEUE_URL = os.getenv("SQS_QUEUE_URL", "https://sqs.us-east-2.amazonaws.com/518870435714/QueueEventPragma")

def get_sqs_client(aws_conn_id="aws_conn_pragma"):
    """
    Returns a boto3 SQS client using AWS credentials stored in an Airflow connection.

    This function retrieves the AWS access key, secret key, and region from the
    specified Airflow connection ID and creates a boto3 SQS client accordingly.

    Parameters:
        aws_conn_id (str): The Airflow connection ID for the AWS credentials.
                           Default is 'aws_conn_pragma'.

    Returns:
        boto3.client: A configured boto3 SQS client instance.

    Raises:
        AirflowNotFoundException: If the specified connection ID does not exist.
        KeyError: If required fields like region are missing from the connection extras.
    """
    conn = BaseHook.get_connection(aws_conn_id)
    aws_access_key = conn.login
    aws_secret_key = conn.password
    region_name = conn.extra_dejson.get("region_name", "us-east-2")

    return boto3.client(
        "sqs",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region_name
    )


def poll_single_sqs_message(**context):
    """Lee un solo mensaje de SQS y lo guarda en XCom si está presente."""
    sqs = get_sqs_client()
    


    response = sqs.receive_message(
        QueueUrl=QUEUE_URL,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=10,
        MessageAttributeNames=['All']
    )

    messages = response.get("Messages", [])
    if not messages:
        print("📭 No hay mensajes nuevos en la cola SQS")
        return  # ← No falla el DAG, simplemente no hace nada

    message = messages[0]
    body = json.loads(message["Body"])
    file_name = body.get("Records", [{}])[0].get("s3", {}).get("object", {}).get("key")

    if not file_name:
        print("⚠️ No se encontró 'file_name' en el mensaje SQS")
        return  # También retornamos sin fallar el DAG

    context['ti'].xcom_push(key="file_name", value=file_name)
    context['ti'].xcom_push(key="receipt_handle", value=message["ReceiptHandle"])
    context['ti'].xcom_push(key="message_id", value=message["MessageId"])

    print(f"📥 Mensaje recibido: {file_name}")


def check_if_file_was_received(**kwargs):
    file_name = kwargs['ti'].xcom_pull(task_ids='poll_sqs', key='file_name')
    if file_name:
        print(f"✅ Archivo detectado en mensaje SQS: {file_name}")
        return True
    else:
        print("📭 No se encontró archivo en el mensaje SQS.")
        return False


def cleanup_single_message(**context):
    """Elimina el mensaje leído de la cola SQS."""
    sqs = get_sqs_client()
    receipt_handle = context['ti'].xcom_pull(task_ids="poll_sqs", key="receipt_handle")
    message_id = context['ti'].xcom_pull(task_ids="poll_sqs", key="message_id")

    if receipt_handle:
        sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle)
        print(f"🧹 Mensaje eliminado de la cola: {message_id}")
