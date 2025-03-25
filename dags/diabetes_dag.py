from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine, inspect
import pandas as pd
import glob
import os

# Define PostgreSQL connection details (Update as per your setup)
POSTGRES_CONN_ID = "airflow_db" 
DB_URI = "postgresql://admin:admin@postgres:5432/airflow_db"

# Path to the adjacent folder containing CSV files
CSV_FOLDER_PATH = "../data/brist1d/"
csv_files = glob.glob(os.path.join(CSV_FOLDER_PATH, "*.csv"))

# Function to load CSV data into PostgreSQL
def load_csv_to_postgres():
    engine = create_engine(DB_URI)
    inspector = inspect(engine)
    with engine.connect() as conn:
    
        for file in csv_files:
            table_name = os.path.basename(file).replace(".csv", "")
            df = pd.read_csv(file)
            
            # Check if table exists
            if table_name in inspector.get_table_names():
                print(f"Table {table_name} exists. Appending data...")

                # Append only new rows efficiently (check by IDs or entire row)
                if 'id' in df.columns:
                    # Append only new rows using SQL query
                    query = f"""
                    INSERT INTO {table_name} 
                    SELECT * FROM tmp
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {table_name} t 
                        WHERE t.id = tmp.id
                    );
                    """
                    df.to_sql('tmp', conn, if_exists='replace', index=False)
                    conn.execute(query)
                    print(f"Appended new rows to {table_name}")
                else:
                    # Append everything if no ID column
                    df.to_sql(table_name, conn, if_exists="append", index=False)
                    print(f"Appended {len(df)} rows into {table_name}")

            else:
                print(f"Table {table_name} does not exist. Creating table and inserting data...")
                df.to_sql(table_name, conn, if_exists="fail", index=False)
                print(f"Created table {table_name} and inserted {len(df)} rows")

# Define Airflow DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 9),
    "retries": 1,
}

dag = DAG(
    "load_csv_to_postgres",
    default_args=default_args,
    schedule_interval="@once", 
    catchup=False,
)

# Task to load CSVs into PostgreSQL
load_csv_task = PythonOperator(
    task_id="load_csv_to_postgres",
    python_callable=load_csv_to_postgres,
    dag=dag,
)

# Task dependencies
# None