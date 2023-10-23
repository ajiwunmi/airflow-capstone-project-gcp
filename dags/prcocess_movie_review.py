from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
# from airflow.providers.google.cloud.operators.dataproc import  DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
# from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    'depends_on_past': False   
}
GCP_CONN_ID = "google_cloud_conn_id"
CLUSTER_NAME = 'de-capstone-cluster'
# REGION='us-central1'
PROJECT_ID='my-capstone-project-401111'
PYSPARK_URI='gs://dataproc-temp-us-east1-700349252747-cikki142/process_movie_reviews.py'


# CLUSTER_CONFIG = {
#     "master_config": {
#         "num_instances": 1,
#         "machine_type_uri": "n1-standard-2",
#         "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 512},
#     },
#     "worker_config": {
#         "num_instances": 2,
#         "machine_type_uri": "n1-standard-2",
#         "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 512},
#     }
# }


PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}

with DAG(
    'dataproc-data-process',
    default_args=default_args,
    description='A simple DAG to create a Dataproc workflow',
    schedule_interval=None,
    # schedule_interval="@once",
    start_date=days_ago(1),
    
) as dag:

    # create_cluster = DataprocCreateClusterOperator(
    #     task_id="create_cluster",
    #     project_id=PROJECT_ID,
    #     cluster_config=CLUSTER_CONFIG,
    ##   region=REGION,
    #     cluster_name=CLUSTER_NAME,
    # )
    start_process = DummyOperator(task_id="start_process")

    submit_job = DataprocSubmitJobOperator(
        task_id="pyspark_task", 
        # gcp_conn_id=GCP_CONN_ID,
        job=PYSPARK_JOB, 
        region='us-east1', 
        project_id=PROJECT_ID
    )

    end_process = DummyOperator(task_id="end_process")
    # delete_cluster = DataprocDeleteClusterOperator(
    #     task_id="delete_cluster", 
    #     project_id=PROJECT_ID, 
    #     cluster_name=CLUSTER_NAME, 
    ##     region=REGION
    # )

    start_process >> submit_job >> end_process