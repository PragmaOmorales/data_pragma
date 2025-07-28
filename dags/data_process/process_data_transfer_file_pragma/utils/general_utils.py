import os
from data_operator.utils.general_utils import (execute_postgres, read_csv_from_s3, get_file_hash_from_s3)

import json
import hashlib
import logging
import polars as pl


# SQL Statements
CHECK_INGESTION_LOG = """
    SELECT 1 FROM ingestion_log WHERE file_name = %s AND file_hash = %s
"""

INSERT_TRANSACTION = """
    INSERT INTO public.transactions (ts, price, user_id, file_name)
    VALUES (%s, %s, %s, %s)
"""

UPDATE_STATS_STATE = """
    UPDATE stats_state
    SET
        row_count = row_count + 1,
        price_sum = price_sum + %s,
        price_min = LEAST(price_min, %s),
        price_max = GREATEST(price_max, %s)
    WHERE id = TRUE
"""

INSERT_REJECTED_ROW = """
    INSERT INTO rejected_rows (file_name, row_data, error)
    VALUES (%s, %s, %s)
"""

INSERT_INGESTION_LOG = """
    INSERT INTO ingestion_log (file_name, file_hash, rows)
    VALUES (%s, %s, %s)
"""

QUERY_STATS = """
    SELECT row_count, price_sum, price_min, price_max
    FROM stats_state
    WHERE id = TRUE
"""

STATS_TRX_QUERY = """
    SELECT COUNT(*) as row_count,
           ROUND(AVG(price), 2) as price_avg,
           MIN(price) as price_min,
           MAX(price) as price_max
    FROM public.transactions
    WHERE file_name = %s
"""

STATS_TOTAL_TRX_QUERY = """
    SELECT COUNT(*) as row_count,
           ROUND(AVG(price), 2) as price_avg,
           MIN(price) as price_min,
           MAX(price) as price_max
    FROM public.transactions
"""

def get_latest_file(folder: str) -> str | None:
    """Returns the most recently modified CSV file in a folder."""
    
    print(f"🔍 Looking for CSV files in: {folder}")

    if not os.path.exists(folder):
        print(f"❌ Folder does not exist: {folder}")
        return None


    csv_files = [f for f in os.listdir(folder) if f.endswith('.csv')]
    if not csv_files:
        return None

    return min(csv_files, key=lambda f: os.path.getmtime(os.path.join(folder, f)))
def file_arrived(folder: str) -> bool:
    """Returns True if there's at least one CSV file in the folder."""
    return get_latest_file(folder) is not None

def decide_processing_path(ti):
    # Obtener file_name desde dag_run.conf (preferido) o desde XCom (fallback)
    file_name = ti.dag_run.conf.get("file_name") if ti.dag_run else None

    # Fallback si no viene desde dag_run.conf
    if not file_name:
        file_name = ti.xcom_pull(task_ids='get_latest_file', key='selected_file')

    if not file_name:
        raise ValueError("❌ No se encontró un archivo para decidir el path.")

    logging.info(f"📦 Evaluando ruta de procesamiento para archivo: {file_name}")

    if file_name == "validation.csv":
        return "validate_validation_file"
    else:
        return "validate_standard_file"

def process_transaction_file(postgres_conn_id: str, file_path: str, file_name: str):
    """Process a CSV file into transactions and update stats_state."""

    schema = {
        "timestamp": pl.Utf8,
        "price": pl.Float64,
        "user_id": pl.Utf8
    }

    full_path = os.path.join(file_path, file_name)
   
    print(f"ruta base archivo {full_path}")

    df = read_csv_from_s3(
        airflow_conn_id='aws_conn_pragma',
        bucket='pragma-omorales',
        key=file_name,
        schema=schema
    )


   
    df = df.with_columns([
        pl.col("price").fill_null(0),
    ])

    print(f"📄 Rows in file: {df.height}")

    s3_path = f"s3://pragma-omorales/{file_name}"  
    file_hash = get_file_hash_from_s3(s3_path, aws_conn_id="aws_conn_pragma")


    print(f"📄 Verificación")
    # Verificar si ya fue procesado
    result, cursor = execute_postgres(
        CHECK_INGESTION_LOG,
        postgres_conn_id,
        with_cursor=True,
        params=(file_name, file_hash)
    )

    if result:
        print(f"⚠️ File {file_name} already processed.")
        return

    for row in df.iter_rows(named=True):
        try:

            print(f'Ejecutando insert transaction {row}')
            execute_postgres(
                INSERT_TRANSACTION,
                postgres_conn_id,
                params=(row["timestamp"], row["price"], row["user_id"], file_name)
            )
            print(f'Ejecutando update transaction {row}')
            execute_postgres(
                UPDATE_STATS_STATE,
                postgres_conn_id,
                params=(row["price"], row["price"], row["price"])
            )
        except Exception as e:
            execute_postgres(
                INSERT_REJECTED_ROW,
                postgres_conn_id,
                params=(file_name, json.dumps(row), str(e), )
            )

    execute_postgres(
        INSERT_INGESTION_LOG,
        postgres_conn_id,
        params=(file_name, file_hash, df.height)
    )

    print("✅ File successfully processed and committed.")


    QUERY_STATS = """
    SELECT row_count, price_sum, price_min, price_max
    FROM stats_state
    WHERE id = TRUE
"""


def print_stats_state(postgres_conn_id: str):
    """Simula el envío de estadísticas stateful a Slack mediante logs."""
    result = execute_postgres(QUERY_STATS, postgres_conn_id)

    if not result:
        logging.warning("⚠️ No se encontraron estadísticas en stats_state.")
        return

    row_count, price_sum, price_min, price_max = result[0]

    avg_price = price_sum / row_count if row_count else 0

    slack_message = f"""
📊 *Estadísticas del procesamiento* (simulado en Slack):
• 🧾 *Total filas procesadas:* `{row_count}`
• 💵 *Promedio de precios:* `{avg_price}`
• 📉 *Precio mínimo:* `{price_min}`
• 📈 *Precio máximo:* `{price_max}`
"""
    logging.info(slack_message)

def print_slack_trx_state(postgres_conn_id: str, file_name: str):
    """Simula el envío de estadísticas de un archivo a Slack mediante logs."""
    logging.info("🔍 Simulating stats summary retrieval for Slack...")

    result = execute_postgres(STATS_TRX_QUERY, postgres_conn_id, params=(file_name,))

    if not result:
        logging.warning(f"⚠️ No stats available for file `{file_name}`.")
        return

    row_count, price_avg, price_min, price_max = result[0]

    slack_message = f"""
📊 *Estadísticas del archivo `{file_name}`* (simulado en Slack):
• 🧾 *Total filas:* `{row_count}`
• 💰 *Promedio precios:* `{price_avg}`
• 🔽 *Precio mínimo:* `{price_min}`
• 🔼 *Precio máximo:* `{price_max}`
"""
    logging.info(slack_message)


def print_slack_total_trx_state(postgres_conn_id: str):
    """Simula el envío de estadísticas de un archivo a Slack mediante logs."""
    logging.info("🔍 Simulating stats summary retrieval for Slack...")

    result = execute_postgres(STATS_TOTAL_TRX_QUERY, postgres_conn_id)

    if not result:
        logging.warning(f"⚠️ No stats available.")
        return

    row_count, price_avg, price_min, price_max = result[0]

    slack_message = f"""
📊 *Estadísticas Totales* (simulado en Slack):
• 🧾 *Total filas:* `{row_count}`
• 💰 *Promedio precios:* `{price_avg}`
• 🔽 *Precio mínimo:* `{price_min}`
• 🔼 *Precio máximo:* `{price_max}`
"""
    logging.info(slack_message)

