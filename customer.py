from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
import pandas as pd
import glob
import os

# Define the URI for PostgreSQL
DW_POSTGRES_URI = "postgresql://intro_final_project_owner:cCx5bnVg1sRd@ep-wispy-paper-a57q9p38.us-east-2.aws.neon.tech/intro_final_project?sslmode=require"

def load_customer_data():
    # Get the directory path from Airflow variables
    directory_path = Variable.get("customer_data_directory_path", default_var="data")
    all_files = glob.glob(os.path.join(directory_path, '*.csv'))
    
    if not all_files:
        raise FileNotFoundError(f"No CSV files found in the directory {directory_path}")

    # Concatenate all CSV files
    df = pd.concat((pd.read_csv(file) for file in all_files), ignore_index=True)
    # Save the combined data to a temporary CSV file
    df.to_csv("/tmp/customer_data.csv", index=False)

def customer_funnel():
    # Connect to PostgreSQL
    hook = PostgresHook(postgres_conn_id="postgres_dw", postgres_conn_uri=DW_POSTGRES_URI)
    engine = hook.get_sqlalchemy_engine()
    
    # Read the temporary CSV file and write it to the PostgreSQL database
    pd.read_csv("/tmp/customer_data.csv").to_sql("customers", engine, if_exists="replace", index=False)

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

# Define the DAG
dag = DAG(
    "ingest_customer_data",
    default_args=default_args,
    description="Ingest customer data",
    schedule_interval="@once",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Define the task to load customer data
task_load_customer_data = PythonOperator(
    task_id="load_customer_data",
    python_callable=load_customer_data,
    dag=dag,
)

# Define the task to ingest customer data to the database
task_ingest_customer_data = PythonOperator(
    task_id="ingest_customer_data",
    python_callable=customer_funnel,
    dag=dag,
)

# Set the task dependencies
task_load_customer_data >> task_ingest_customer_data
