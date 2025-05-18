from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd
import glob
import os
import logging
import psycopg2
import gc
import psutil

logging.basicConfig(level=logging.INFO)
logging.info("DAG Start")
logging.info(f"Memory usage before insert: {psutil.virtual_memory().percent}%")

# PostgreSQL connection details
POSTGRES_CONN_ID = "airflow_db"
DB_URI = "postgresql://admin:admin@postgres:5432/airflow_db"

# Path to CSV files (inside container)
CSV_FOLDER_PATH = "/opt/airflow/data/brist1d/"
csv_files = glob.glob(os.path.join(CSV_FOLDER_PATH, "*.csv"))
logging.info(f"CSV files found: {csv_files}")

# Define chunk size for data processing
CHUNK_SIZE = 1000

def quote_column_name(col):
    """Wrap column names in double quotes to preserve special characters"""
    return f'"{col.strip()}"'

def load_csv_to_postgres():
    logging.info("::Running load_csv_to_postgres")

    # Connect to PostgreSQL
    engine = create_engine(DB_URI)
    conn = psycopg2.connect(DB_URI)
    cursor = conn.cursor()

    for file in csv_files:
        table_name = os.path.basename(file).replace(".csv", "")
        logging.info(f"::Processing {file} into table {table_name}")

        # Stream CSV in chunks
        with pd.read_csv(file, chunksize=CHUNK_SIZE) as reader:
            
            # Get columns and add quotes
            first_chunk = next(reader)
            quoted_columns = [quote_column_name(col) for col in first_chunk.columns]

            # Create table if doesn't exist
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join([f'{col} TEXT' for col in quoted_columns])}
            );
            """
            cursor.execute(create_table_query)
            conn.commit()

            # Insert unique rows into table by chunks
            for i, chunk in enumerate(pd.read_csv(file, chunksize=CHUNK_SIZE)):
                logging.info(f"::Inserting chunk {i + 1} into {table_name}")

                # Write chunk to a temp file
                chunk_file = f"/tmp/{table_name}_chunk_{i}.csv"
                chunk.to_csv(chunk_file, index=False, header=True)

                # Create temp table
                temp_table = f"{table_name}_temp"
                cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
                cursor.execute(f"CREATE TEMP TABLE {temp_table} AS TABLE {table_name} WITH NO DATA")
                conn.commit()

                # Stream chunk into Postgres
                with open(chunk_file, "r") as f:
                    cursor.copy_expert(f"COPY {temp_table} FROM STDIN WITH CSV HEADER", f)

                # Merge unique rows
                merge_query = f"""
                INSERT INTO {table_name} ({', '.join(quoted_columns)})
                SELECT {', '.join(quoted_columns)}
                FROM {temp_table}
                ON CONFLICT DO NOTHING;
                """
                cursor.execute(merge_query)
                conn.commit()

                # Cleanup
                os.remove(chunk_file)
                logging.info(f"::Inserted chunk {i + 1} into {table_name}")

                # Force garbage collection
                del chunk
                gc.collect()

            logging.info(f"::Inserted all unique rows into {table_name}")

    cursor.close()
    conn.close()

# Define Airflow DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
    "execution_timeout": timedelta(hours=3),
}

dag = DAG(
    "load_csv_to_postgres",
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
)

# Task to load CSVs into Postgres
load_csv_task = PythonOperator(
    task_id="load_csv_to_postgres",
    python_callable=load_csv_to_postgres,
    dag=dag,
)
