"""Database Ingestion Workflow

Author: Enrique Olivares <enrique.olivares@wizeline.com>

Description: Ingests the data from a GCS bucket into a postgres table.
"""

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.sql import BranchSQLOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from datetime import datetime
import pandas as pd
from io import StringIO




# General constants
DAG_ID = "gcp_database_ingestion_workflow"
STABILITY_STATE = "unstable"
CLOUD_PROVIDER = "gcp"

# GCP constants
GCP_CONN_ID = "google_cloud_conn_id"
GCS_BUCKET_NAME = "de-captone-poject-bucket"
GCS_KEY_NAME = "dataset/user_purchase.csv"
GCS_FILE_NAME = "/dataset/user_purchase.csv"
GCS_STAGING_FILE_NAME = "/staging/user_purchase.csv"

# Postgres constants
POSTGRES_CONN_ID = "postgres_conn_id_2"
POSTGRES_TABLE_NAME = "user_purchase"
SCHEMA_NAME = "decapstone"


def ingest_data_from_gcs(
    gcs_bucket: str,
    gcs_object: str,
    postgres_table: str,
    gcp_conn_id: str = "google_cloud_default",
    postgres_conn_id: str = "postgres_default",
):
    """Ingest data from an GCS location into a postgres table.

    Args:
        gcs_bucket (str): Name of the bucket.
        gcs_object (str): Name of the object.
        postgres_table (str): Name of the postgres table.
        gcp_conn_id (str): Name of the Google Cloud connection ID.
        postgres_conn_id (str): Name of the postgres connection ID.
    """
    import tempfile

    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
    psql_hook = PostgresHook(postgres_conn_id)

    with tempfile.NamedTemporaryFile() as tmp:
        gcs_hook.download(
            bucket_name=gcs_bucket, object_name=gcs_object, filename=tmp.name
        )
        psql_hook.bulk_load(table=postgres_table, tmp_file=tmp.name)




# Define a function to perform data wrangling
def data_wrangling():
    # Read the CSV file from Google Cloud Storage
    gcs_to_local_task = GCSToLocalFilesystemOperator(
        task_id='read_gcs_data',
        bucket_name=GCS_BUCKET_NAME,
        object_name=GCS_KEY_NAME,
        filename=GCS_FILE_NAME,
        gcp_conn_id=GCP_CONN_ID,
        dag=dag,
    )
    
    # Load data from the local file and perform data wrangling
    file_path = GCS_FILE_NAME
    df = pd.read_csv(file_path)

    # Data wrangling steps
    df = df.dropna()  # Remove null values
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], format='%m/%d/%Y %H:%M').dt.strftime('%Y-%m-%d %H:%M')

    # Store the cleaned data back to a CSV file (you can modify this to store in a different format)
    df.to_csv(GCS_STAGING_FILE_NAME, index=False)
    cleaned_data = df.to_csv(index=False, sep=',', quoting=2, escapechar='\\', quotechar='"', encoding='utf-8')
    # cleaned_data = StringIO(cleaned_data)

   

with DAG(
    dag_id=DAG_ID,
    schedule_interval="@once",
    start_date=days_ago(1),
    tags=[CLOUD_PROVIDER, STABILITY_STATE],
) as dag:
    
    start_workflow = DummyOperator(task_id="start_workflow")

    verify_key_existence = GCSObjectExistenceSensor(
        task_id="verify_key_existence",
        google_cloud_conn_id=GCP_CONN_ID,
        bucket=GCS_BUCKET_NAME,
        object=GCS_KEY_NAME,
    )

    # Perform data wrangling
    data_wrangling= PythonOperator(
        task_id='data_wrangling',
        python_callable=data_wrangling,
        # provide_context=True,
        dag=dag,
        # trigger_rule=TriggerRule.ONE_SUCCESS,
    )


    # Define schema and table creation SQL queries
    create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};"
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{POSTGRES_TABLE_NAME}  (
            invoice_number VARCHAR,
            stock_code VARCHAR(30),
            detail VARCHAR(1000),
            quantity INT,
            invoice_date TIMESTAMP,
            unit_price NUMERIC(8,3),
            customer_id INT,
            country VARCHAR(30)
        );
    """

    create_table_entity = PostgresOperator(
        task_id="create_table_entity",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=[create_schema_query,create_table_query],
    )

    clear_table = PostgresOperator(
        task_id="clear_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"DELETE FROM {SCHEMA_NAME}.{POSTGRES_TABLE_NAME}",
    )
    continue_process = DummyOperator(task_id="continue_process")

    # Ingest data from GCS to Postgres
    ingest_data = PythonOperator(
        task_id="ingest_data",
        python_callable=ingest_data_from_gcs,
        op_kwargs={
            "gcp_conn_id": GCP_CONN_ID,
            "postgres_conn_id": POSTGRES_CONN_ID,
            "gcs_bucket": GCS_BUCKET_NAME,
            "gcs_object": GCS_KEY_NAME,
            "postgres_table":f"{SCHEMA_NAME}.{POSTGRES_TABLE_NAME}",
        },
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # load_data_task = PythonOperator(
    # task_id='load_data_to_postgres',
    # python_callable=load_data_to_postgres,
    # dag=dag,
    # )


    validate_data = BranchSQLOperator(
        task_id="validate_data",
        conn_id=POSTGRES_CONN_ID,
        sql=f"SELECT COUNT(*) AS total_rows FROM {SCHEMA_NAME}.{POSTGRES_TABLE_NAME}",
        follow_task_ids_if_false=[continue_process.task_id],
        follow_task_ids_if_true=[clear_table.task_id],
    )

    end_workflow = DummyOperator(task_id="end_workflow")

    (
        start_workflow
        >> verify_key_existence
        >> create_table_entity
        >> validate_data
    )
    validate_data >> [clear_table, continue_process] >> data_wrangling >> ingest_data
    data_wrangling >> ingest_data >> end_workflow

    dag.doc_md = __doc__
