from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from datetime import datetime
import requests
import os

# Define the URL, GCP bucket, and object name
# URL   = url= 'https://github.com/ajiwunmi/airflow-capstone-project-gcp/data/movie_reviews.csv'
URL   = url= 'https://github.com/ajiwunmi/airflow-capstone-project-gcp/data/movie_reviews.csv'

BUCKET_NAME = bucket_name= 'de-captone-poject-bucket'
OBJECT_NAME = object_name = 'movie_reviews.csv'
local_path = '/tmp/movie_reviews.csv'


def download_file(url, local_path):
    """
    Function to download a file from a URL and save it locally.
    """
    response = requests.get(url)
    with open(local_path, 'wb') as f:
        f.write(response.content)

def upload_to_gcp_bucket(local_path, bucket_name, object_name):
    """
    Function to upload a file from a local path to a GCP bucket.
    """
    gcs_hook = GoogleCloudStorageHook()
    gcs_hook.upload(bucket_name, object_name, local_path)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 10),
    'retries': 1,
}

dag = DAG(
    'upload_to_gcp_bucket',
    default_args=default_args,
    description='A DAG to upload files from a URL to a GCP bucket',
    schedule_interval='@daily',  # Set the schedule as needed
)



# Define the tasks in the DAG
with dag:
    task1 = PythonOperator(
        task_id='download_file',
        python_callable=download_file,
        op_args=[URL, local_path],
    )

    task2 = PythonOperator(
        task_id='upload_to_gcp_bucket',
        python_callable=upload_to_gcp_bucket,
        op_args=[local_path, BUCKET_NAME, OBJECT_NAME],
    )

# Define the task dependencies
task1 >> task2
