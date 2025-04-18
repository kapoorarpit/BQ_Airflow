from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator 
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator 
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.dates import days_ago 
from google.cloud import storage, bigquery
from airflow.utils.dates import days_ago
import json


# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}


def transform_to_json(ti):
    # Retrieve data from XCom
    query_data = ti.xcom_pull(task_ids='query_data_task', key='return_value')
    # Example transformation: Convert names to uppercase
    # transformed_data = [{'name': row['name'].upper()} for row in query_data]
    transformed_data = [{'name': row[0].upper()} for row in query_data]
    # Convert to JSON and push to XCom
    json_data = json.dumps(transformed_data, indent=2)
    ti.xcom_push (key='json_data', value=json_data)

def upload_to_gcs(ti):
    # Retrieve JSON data from XCom
    json_data = ti.xcom_pull(task_ids='transform_to_json_task', key='json_data')
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")


    bucket_name = 'bq_astronomer'
    destination_blob_name = 'transformed_data.json' # • Define your destination file name
    # Access the bucket and upload the data
    gcs_hook.upload(
        bucket_name=bucket_name,
        object_name=destination_blob_name,
        data=json_data,
        mime_type='application/json'
    )


with DAG(
    'bigquery_to_gcs_dag',
    default_args=default_args,
    description='A DAG that queries, transforms, and uploads BigQuery data to GCS', 
    schedule_interval=None, # Run manually or specify your schedule start_date=days_ago(1),
) as dag:
    
    query_data_task = BigQueryInsertJobOperator(
        task_id="query_data_task",
        configuration={
            "query": {
                "query": """
                    SELECT DISTINCT name FROM `bigquery-public-data.covid19_italy.data_by_province`
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id="google_cloud_default",
    )

    # Task 2: Transform data to JSON
    transform_to_json_task = PythonOperator(
    task_id='transform_to_json_task',
    python_callable=transform_to_json,
    )

    # Task 3: Upload JSON to GCS
    upload_to_gcs_task = PythonOperator(
    task_id='upload_to_gcs_task',
    python_callable=upload_to_gcs,
    )

    # Define task dependencies
    query_data_task >> transform_to_json_task >> upload_to_gcs_task