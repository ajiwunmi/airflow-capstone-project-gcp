"""Database Ingestion Workflow

Author: Enrique Olivares <enrique.olivares@wizeline.com>

Description: Ingests the data from a GCS bucket into a postgres table.
"""
# pip install gcsfs
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
# from airflow.providers.google.cloud.operators.gcs import GoogleCloudStorageToCsvOperator

from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from datetime import datetime
import pandas as pd
from io import StringIO
# import gcsfs
import tempfile



PROJECT_NAME ="my-capstone-project"
# General constants
DAG_ID = "gcp_database_ingestion_workflow"
STABILITY_STATE = "unstable"
CLOUD_PROVIDER = "gcp"

# GCP constants
GCP_CONN_ID = "google_cloud_conn_id"
GCS_BUCKET_NAME = "de-captone-poject-bucket"
GCS_KEY_NAME = "dataset/user_purchase.csv"
GCS_FILE_NAME = "dataset/user_purchase.csv" 
GCS_FILE_NAME  = GCS_INGEST_DATA = "staging_area/cleaned_user_purchase.csv" 
TEMP_FILE_NAME = "tmp/user_purchase.csv" 
#gs://de-captone-poject-bucket/dataset/user_purchase.csv | https://storage.cloud.google.com/de-captone-poject-bucket/dataset/user_purchase.csv
GCS_STAGING_FILE_NAME = "staging_area/user_purchase.csv"

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
    

    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
    psql_hook = PostgresHook(postgres_conn_id)

    with tempfile.NamedTemporaryFile() as tmp:
        gcs_hook.download(
            bucket_name=gcs_bucket, object_name=gcs_object, filename=tmp.name
        )
        # psql_hook.bulk_load(table=postgres_table, tmp_file=tmp.name)
        psql_hook.copy_expert(sql=postgres_table, filename=tmp.name)
         # Define a Postgres operator to copy data into the PostgreSQL table
        


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

    # # Define a Postgres operator to copy data into the PostgreSQL table
    # data_wrangling = PostgresOperator(
    #         task_id='copy_to_postgres',
    #         sql=f'COPY {SCHEMA_NAME}.user_purchase FROM stdin CSV HEADER',
    #         parameters={'table_name': 'user_purchase'},
    #         conn_id=POSTGRES_CONN_ID,
    #         data=data_wrangling(),
            
    # )
    # pg_operator.execute()
    # Perform data wrangling
    # data_wrangling= PythonOperator(
    #     task_id='data_wrangling',
    #     python_callable=data_wrangling,
    #     trigger_rule=TriggerRule.ONE_SUCCESS,
    #     provide_context=True,
    # )

    # Set up task dependencies
    # data_wrangling_task = PythonOperator(
    #     task_id='data_wrangling_task',
    #     python_callable=data_wrangling,
    #     provide_context=True,
    #     dag=dag,
    # )

    # Define schema and table creation SQL queries
    create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};"
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{POSTGRES_TABLE_NAME}  (
            invoice_number VARCHAR DEFAULT 1,
            stock_code VARCHAR(30) DEFAULT '',
            detail VARCHAR(1000) DEFAULT '',
            quantity INT DEFAULT 0,
            invoice_date TIMESTAMP,
            unit_price NUMERIC(8,3) DEFAULT 1,
            customer_id VARCHAR DEFAULT 1,
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

    drop_table_if_exists = PostgresOperator(
        task_id="drop_table_if_exists",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"DROP TABLE IF EXISTS {SCHEMA_NAME}.{POSTGRES_TABLE_NAME}",
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
            "gcs_object": GCS_INGEST_DATA, #GCS_KEY_NAME
            "postgres_table":f'COPY {SCHEMA_NAME}.user_purchase FROM stdin CSV HEADER', #f"{SCHEMA_NAME}.{POSTGRES_TABLE_NAME}",
            
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
        # follow_task_ids_if_false=[clear_table.task_id],
        follow_task_ids_if_true=[clear_table.task_id],
    )

    end_workflow = DummyOperator(task_id="end_workflow")

    (
        start_workflow
        >> verify_key_existence
        >> drop_table_if_exists
        >> create_table_entity
        >> validate_data
    )
    validate_data >> [clear_table, continue_process]  >> ingest_data
    ingest_data >> end_workflow

    dag.doc_md = __doc__
