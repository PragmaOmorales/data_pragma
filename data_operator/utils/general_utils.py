from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
import boto3
from contextlib import closing
import s3fs
import json
import polars as pl
from airflow.hooks.base import BaseHook
import hashlib



def execute_postgres(query, postgres_conn_id, with_cursor=False, params=None):
    hook = PostgresHook(postgres_conn_id)
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        cur.execute(query, params)
        if cur.description is not None:
            res = cur.fetchall()
        else:
            res = None
        conn.commit()
        if with_cursor:
            return res, cur
        return res
    finally:
        cur.close()
        conn.close()




def read_csv_from_s3(airflow_conn_id: str, bucket: str, key: str, schema: dict) -> pl.DataFrame:
    """
    Reads a CSV file from an S3 bucket using Airflow's AWS connection and returns a Polars DataFrame.

    This function uses credentials stored in an Airflow connection to securely access
    a file in S3, and parses it with Polars using the provided schema. It automatically
    handles null values in the "price" column by filling them with 0.

    Parameters:
    ----------
    airflow_conn_id : str
        The Airflow connection ID for an AWS connection.
    bucket : str
        The name of the S3 bucket (without the 's3://' prefix).
    key : str
        The object key (file path) within the S3 bucket.
    schema : dict
        A dictionary defining the schema to be used when reading the CSV with Polars.

    Returns:
    -------
    pl.DataFrame
        A Polars DataFrame containing the parsed CSV data.

    Raises:
    ------
    FileNotFoundError:
        If the file does not exist in the specified bucket.
    Exception:
        If there are errors during file reading or connection.
    """

    conn = BaseHook.get_connection(airflow_conn_id)
    
    extra = json.loads(conn.extra) if conn.extra else {}
    region = extra.get("region_name", "us-east-1")  # default

    fs = s3fs.S3FileSystem(
        key=conn.login,
        secret=conn.password,
        client_kwargs={'region_name': region}
    )

    s3_path = f"{bucket}/{key}"  

    with fs.open(s3_path, mode='rb') as f:
        df = pl.read_csv(f, schema=schema)

    df = df.with_columns([
        pl.col("price").fill_null(0),
    ])

    return df

def get_file_hash_from_s3(s3_path: str, aws_conn_id: str = "aws_conn_pragma") -> str:
    """
    Calculates the SHA256 hash of a file stored in S3.

    Args:
        s3_path (str): Full S3 path (e.g., 's3://bucket/key.csv')
        aws_conn_id (str): Airflow connection ID with AWS credentials

    Returns:
        str: SHA256 hex digest of the file content
    """
    conn = BaseHook.get_connection(aws_conn_id)
    session = boto3.session.Session(
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        region_name=conn.extra_dejson.get("region_name", "us-east-2")
    )

    s3 = session.client("s3")

    # Extract bucket and key
    path = s3_path.replace("s3://", "")
    bucket, key = path.split("/", 1)

    obj = s3.get_object(Bucket=bucket, Key=key)
    file_bytes = obj["Body"].read()

    return hashlib.sha256(file_bytes).hexdigest()


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