import os
import logging
from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import requests

# Constants
bucket  = 'de-captone-poject-bucket'
dataset_url_1 = "https://data.montgomerycountymd.gov/resource/v76h-r7br.csv"
dataset_file_1 = "warehouse_and_details_sales.csv"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

def download_samples_from_url(url, output_path):
    """Downloads a file from the specified URL and saves it to the output path.

    Args:
        url (str): URL to download the file from.
        output_path (str): Path to save the downloaded file.
    """
    response = requests.get(url)
    with open(output_path, mode="wb") as file:
        file.write(response.content)

def upload_file_to_gcs(bucket_name, object_name, local_file_path):
    """Uploads a file to GCS.

    Args:
        bucket_name (str): GCS bucket name.
        object_name (str): Name to be given to the object in GCS.
        local_file_path (str): Local path to the file to be uploaded.
    """
    hook = GCSHook(gcp_conn_id='google_cloud_conn')
    hook.upload(bucket_name, object_name, local_file_path)

with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['upload-gcs']
) as dag:

    # Task to download the file from the URL
    download_file_task = PythonOperator(
        task_id="download_file",
        python_callable=download_samples_from_url,
        op_args=[dataset_url_1, dataset_file_1]
    )

    # Task to upload the file to GCS
    upload_to_gcs_task = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_file_to_gcs,
        op_args=[bucket, dataset_file_1, Path(dataset_file_1)]
    )

    download_file_task >> upload_to_gcs_task  # Set task dependencies
