from airflow import models
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import  DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.dummy import DummyOperator


GCP_CONN_ID = "google_cloud_conn_id"
CLUSTER_NAME = 'capstone-cluster-1'
REGION='us-central1' # region
PROJECT_ID='my-capstone-project-401111' #project name
PYSPARK_URI='gs://de-captone-poject-bucket/spark_scripts/process_movie_reviews.py' # spark job location in cloud storage


CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    }
}


PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}

with models.DAG(
    "dataproc_airflow_data_process_gcp",
    schedule_interval=None,  
    start_date=days_ago(1),
    catchup=False,
    tags=["dataproc_airflow"],
) as dag:

    start_process = DummyOperator(task_id="start_process")
    # create_cluster = DataprocCreateClusterOperator(
    #     task_id="create_cluster",
    #     project_id=PROJECT_ID,
    #     cluster_config=CLUSTER_CONFIG,
    # #    region=REGION,
    #     cluster_name=CLUSTER_NAME,
    # )

    submit_job = DataprocSubmitJobOperator(
        task_id="pyspark_task", 
        job=PYSPARK_JOB, 
        region=REGION, 
        project_id=PROJECT_ID,
        gcp_conn_id=GCP_CONN_ID,
        impersonation_chain='de-capstone-service-account@my-capstone-project-401111.iam.gserviceaccount.com'
    )

    gsc_to_gbq = GCSToBigQueryOperator(
        task_id="transfer_data_to_bigquery",
        bucket="gs://de-captone-poject-bucket/staging_area",
        source_objects =["classified_movie_review.csv"],
        destination_project_dataset_table ="classified_table", # bigquery table
        source_format = "PARQUET"
    )

    # delete_cluster = DataprocDeleteClusterOperator(
    #     task_id="delete_cluster", 
    #     project_id=PROJECT_ID, 
    #     cluster_name=CLUSTER_NAME, 
    # #   region=REGION
    # )

    end_process = DummyOperator(task_id="end_process")

    start_process >> submit_job >> end_process

    # create_cluster >> submit_job >> [delete_cluster,gsc_to_gbq]