from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# Define your default_args
default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 20),
    'retries': 1,
}

# Create a DAG
dag = DAG('process_movie_reviews', default_args=default_args, schedule_interval=None)

# Define your PySpark job parameters
spark_submit_task = SparkSubmitOperator(
    task_id='process_movie_reviews_task',
    application='gs://dataproc-temp-us-east1-700349252747-cikki142/process_movie_reviews.py',  # Path to your PySpark job script
    conn_id='spark_default',  # Name of the Spark connection defined in Airflow
    verbose=False,  # Set to True to see Spark job logs in Airflow UI
    dag=dag,
    conf={
        'spark.master': 'yarn',  # Set to your Spark master URL
        'spark.submit.deployMode': 'client',  # Or 'cluster' if needed
        'spark.driver.memory': '1g',  # Adjust as needed
        'spark.executor.memory': '2g',  # Adjust as needed
    },
)

# Set up the task dependencies
spark_submit_task
