from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.operators.gcs_to_postgres import GoogleCloudStorageToPostgresOperator

# Define DAG details
dag_name = 'user_purchase_dag'
schedule_interval = '@daily'  # Adjust as needed
dag_start_date = datetime(2023, 10, 11)

# Define PostgreSQL connection details
postgres_conn_id = 'your_postgres_connection_id'

# Define schema and table creation SQL queries
schema_name = 'your_schema_name'
create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.user_purchase (
        invoice_number VARCHAR(20),
        stock_code VARCHAR(20),
        detail VARCHAR(1000),
        quantity INT,
        invoice_date TIMESTAMP,
        unit_price NUMERIC(8,3),
        customer_id INT,
        country VARCHAR(30)
    );
"""

# Define GCS to PostgreSQL import details
gcs_bucket_name = 'your_gcs_bucket_name'
gcs_file_path = 'user_purchase.csv'
table_name = f'{schema_name}.user_purchase'

# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dag_start_date,
    'retries': 1,
}

# Create the DAG
dag = DAG(
    dag_name,
    default_args=default_args,
    description='A DAG to import user_purchase data into PostgreSQL from GCS',
    schedule_interval=schedule_interval,
)

# Task 1: Connect to PostgreSQL and create table
create_table_task = PostgresOperator(
    task_id='create_user_purchase_table',
    postgres_conn_id=postgres_conn_id,
    sql=[create_schema_query, create_table_query],
    dag=dag,
)

# Task 2: Import user_purchase.csv from GCS to PostgreSQL
import_csv_task = GoogleCloudStorageToPostgresOperator(
    task_id='import_user_purchase_csv',
    postgres_conn_id=postgres_conn_id,
    schema=schema_name,
    table=table_name,
    bucket_name=gcs_bucket_name,
    filename=gcs_file_path,
    field_delimiter=',',
    google_cloud_storage_conn_id='google_cloud_storage_default',
    export_format='CSV',
    field_optionally_enclosed_by='"',
    do_xcom_push=False,  # Set to True if you need XCom communication
    skip_leading_rows=1,
    dag=dag,
)

# Define task dependencies
create_table_task >> import_csv_task



from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Import PySpark-related libraries
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import current_timestamp, col, when

# Define the DAG
dag = DAG(
    'movie_review_processing_dag',
    description='Process movie_review.csv',
    schedule_interval='@daily',  # Adjust as needed
    start_date=datetime(2023, 10, 11),
    catchup=False
)

# Define the Python function to process the CSV file using PySpark
def process_movie_review():
    spark = SparkSession.builder \
        .appName("MovieReviewProcessing") \
        .getOrCreate()

    # Load the movie_review.csv into a DataFrame
    df = spark.read.csv("gs://your_bucket/movie_review.csv", header=True, inferSchema=True)

    # Tokenize the review_str column
    tokenizer = Tokenizer(inputCol="review_str", outputCol="review_token")
    df = tokenizer.transform(df)

    # Remove stop words
    remover = StopWordsRemover(inputCol="review_token", outputCol="filtered_tokens")
    df = remover.transform(df)

    # Check if the review contains the word "good" and label as positive review
    df = df.withColumn("positive_review", when(col("review_str").contains("good"), 1).otherwise(0))

    # Add a timestamp column for insert_date
    df = df.withColumn("insert_date", current_timestamp())

    # Select the required columns
    result_df = df.select("cid", "positive_review", "review_id")

    # Save the results to a new file in the STAGE_area folder
    result_df.write.mode("overwrite").csv("gs://your_bucket/STAGE_area", header=True)

    # Stop the Spark session
    spark.stop()

# Define the PythonOperator to execute the PySpark script
process_movie_review_task = PythonOperator(
    task_id='process_movie_review',
    python_callable=process_movie_review,
    dag=dag
)

# Set task dependencies
process_movie_review_task




from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator

# Define the DAG
dag = DAG(
    'postgres_to_gcs_dag',
    description='Download PostgreSQL data and store in GCS',
    schedule_interval='@daily',  # Adjust as needed
    start_date=datetime(2023, 10, 11),
    catchup=False
)

# Define PostgreSQL connection details
postgres_conn_id = 'your_postgres_connection_id'
schema_name = 'your_schema_name'

# Define the task to extract data from PostgreSQL and store in GCS
extract_postgres_to_gcs_task = PostgresToGoogleCloudStorageOperator(
    task_id='extract_postgres_to_gcs',
    sql=f'SELECT * FROM {schema_name}.user_purchase',
    bucket_name='your_bucket_name',
    filename='STAGE_area/user_purchase_data.csv',  # Adjust the filename/path as needed
    postgres_conn_id=postgres_conn_id,
    export_format='CSV',
    field_delimiter=',',
    skip_leading_rows=0,
    dag=dag
)

# Set task dependencies
extract_postgres_to_gcs_task


