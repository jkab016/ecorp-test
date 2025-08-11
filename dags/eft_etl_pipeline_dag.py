from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import os
import sys
import pandas as pd

# Make sure our src modules are importable
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

from modules.ingest import ingest_csv_to_staging
from modules.transform import transform_bank_txns, transform_customer_txns
from modules.load import load_to_mysql
from modules.utils import get_engine, setup_logging

# get airflow variables for configuration
DATA_PATH = Variable.get("eft_data_path")
DB_USER = Variable.get("eft_db_user")
DB_PASS = Variable.get("eft_db_pass")
DB_NAME = Variable.get("eft_db_name")
DB_HOST = Variable.get("eft_db_host", default_var="host.docker.internal")
DB_PORT = Variable.get("eft_db_port", default_var="3306")

logger = setup_logging()
engine = get_engine(DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME)

# create task functions for each step in the ETL pipeline
def airflow_ingest():
    """Truncate + Load CSV into staging table"""
    ingest_csv_to_staging(engine, DATA_PATH, "stg_transactions", logger)

def airflow_transform_bank():
    """Read staging, transform bank summary, save to temp parquet"""
    df_staging = pd.read_sql("SELECT * FROM stg_transactions", engine)
    bank_df = transform_bank_txns(df_staging, logger)
    bank_df.to_parquet("/opt/airflow/tmp/bank.parquet", index=False)

def airflow_transform_customer():
    """Read staging, transform customer summary, save to temp parquet"""
    df_staging = pd.read_sql("SELECT * FROM stg_transactions", engine)
    customer_df = transform_customer_txns(df_staging, logger)
    customer_df.to_parquet("/opt/airflow/tmp/customer.parquet", index=False)

def airflow_load_bank():
    """Load bank summary into analytics table"""
    df = pd.read_parquet("/opt/airflow/tmp/bank.parquet")
    load_to_mysql(engine, df, "ana_bank_daily_summary", logger, entity_name="BANK")

def airflow_load_customer():
    """Load customer summary into analytics table"""
    df = pd.read_parquet("/opt/airflow/tmp/customer.parquet")
    load_to_mysql(engine, df, "ana_customer_daily_summary", logger, entity_name="CUSTOMER")

# airflow DAG definition with pseudo email id, vary the email as needed
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["test@mail.com"],  
    "email_on_failure": True,
    "retries": 1
}

with DAG(
    dag_id="eft_etl_pipeline",
    default_args=default_args,
    description="Simple ETL pipeline for EFT transactions",
    start_date=datetime(2025, 8, 8),
    schedule_interval="@daily",
    catchup=False,
    tags=["ETL", "EFT"],
) as dag:

    task_ingest = PythonOperator(
        task_id="ingest_to_staging",
        python_callable=airflow_ingest
    )

    task_transform_bank = PythonOperator(
        task_id="transform_bank",
        python_callable=airflow_transform_bank
    )

    task_transform_customer = PythonOperator(
        task_id="transform_customer",
        python_callable=airflow_transform_customer
    )

    task_load_bank = PythonOperator(
        task_id="load_bank_summary",
        python_callable=airflow_load_bank
    )

    task_load_customer = PythonOperator(
        task_id="load_customer_summary",
        python_callable=airflow_load_customer
    )

    # Stiching the tasks together: ingest>>transformations for bank and customer>>load to analytics
    task_ingest >> [task_transform_bank, task_transform_customer]
    task_transform_bank >> task_load_bank
    task_transform_customer >> task_load_customer
