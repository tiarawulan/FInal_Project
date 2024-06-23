from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
import pandas as pd
import glob
import os
import json

# Mendapatkan URI PostgreSQL dari variabel Airflow
DW_POSTGRES_URI="postgresql://intro_final_project_owner:cCx5bnVg1sRd@ep-wispy-paper-a57q9p38.us-east-2.aws.neon.tech/intro_final_project?sslmode=require"

def load_coupons():
    # Mendapatkan path direktori dari variabel Airflow
    directory_path = Variable.get("coupons_directory_path", default_var="data")
    all_files = glob.glob(os.path.join(directory_path, '*.json'))
    
    if not all_files:
        raise FileNotFoundError(f"Tidak ada file json ditemukan di direktori {directory_path}")

    # Menggabungkan semua file JSON
    df = pd.concat((pd.read_json(file) for file in all_files), ignore_index=True)
    # Menyimpan data gabungan ke file JSON sementara
    df.to_json("/tmp/coupons.json", orient='records', lines=True)

def coupons_funnel():
    # Menghubungkan ke PostgreSQL
    hook = PostgresHook(postgres_conn_id="postgres_dw", postgres_conn_uri=DW_POSTGRES_URI)
    engine = hook.get_sqlalchemy_engine()
    
    # Membaca file JSON sementara dan menulisnya ke database PostgreSQL
    pd.read_json("/tmp/coupons.json", lines=True).to_sql("coupons", engine, if_exists="replace", index=False)

# Argumen default untuk DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

# Definisi DAG
dag = DAG(
    "ingest_coupons",
    default_args=default_args,
    description="Ingesti coupons",
    schedule_interval="@once",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Definisi tugas untuk memuat data coupons
task_load_coupons = PythonOperator(
    task_id="load_coupons",
    python_callable=load_coupons,
    dag=dag,
)

# Definisi tugas untuk mengingest data coupons ke database
task_ingest_coupons = PythonOperator(
    task_id="ingest_coupons",
    python_callable=coupons_funnel,
    dag=dag,
)

# Menentukan urutan eksekusi tugas
task_load_coupons >> task_ingest_coupons
