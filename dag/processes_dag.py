import pandas as pd
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from google.cloud import storage
from google.cloud import bigquery

DAG_NAME = "processes_dag"

default_args = {
    "depends_on_past": False,
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "owner": "airflow",
    "retries": 3,
    "relay_delay": timedelta(minutes=2),
    "start_date": datetime(2017, 11, 1)
}

dag = DAG(
    dag_id="processes_dag",
    default_args=default_args,
    catchup=False,
    description="processes_dag",
    max_active_runs=5
)

def get_combined_records():
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('golden-union-319717-test1')
    blobs = bucket.list_blobs()
    combined_df = pd.DataFrame()

    for blob in blobs:
        filename = blob.name.replace('/','_')
        print(f"Downloading file {filename}")

        blob.download_to_filename(f"/home/airflow/gcs/data/egen/{filename}")
        print(f"Concatenate {filename} together into a single Dataframe")
        file_df = pd.read_csv(f"/home/airflow/gcs/data/egen/{filename}")
        combined_df = combined_df.append(file_df)

        print(f"Deleting file {filename}")
        blob.delete()

    if len(combined_df) > 0:
        combined_filename = f"combined_files.csv"
        combined_df.to_csv(f'/home/airflow/gcs/data/egen/{combined_filename}', index=False)
        print("Found files moving ahead...")
        return "clean_and_process_records_task"

    else:
        print("No files found, ending this run")
        return "end"

def clean_and_process_records():
    df = pd.read_csv('/home/airflow/gcs/data/egen/combined_files.csv')
    print("Renaming columns")
    df.rename(columns={"p":"price","s":"symbol","b":"bid","a":"ask","t":"timestamp"})
    df.to_csv('/home/airflow/gcs/data/egen/cleaned_files.csv', index=False)

def upload_to_bigquery():
    client = bigquery.Client()

    table_id = "golden-union-319717.golden_union_319717_egen.forex_table"
    destination_table = client.get_table(table_id)

    rows_before_insert = destination_table.num_rows
    print(f"rows before insert: {rows_before_insert}")

    if rows_before_insert > 0:
        disposition = bigquery.WriteDisposition.WRITE_APPEND
        print(f"rows before insert: {rows_before_insert} i.e > 0 disposition is {disposition}")
    elif rows_before_insert == 0:
        disposition = bigquery.WriteDisposition.WRITE_EMPTY
        print(f"rows before insert: {rows_before_insert} i.e = 0 disposition is {disposition}")

    job_config = bigquery.LoadJobConfig(
        write_disposition=disposition,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True
    )

    uri= f'gs://us-central1-egen-module2-068902e1-bucket/data/egen/cleaned_files.csv'

    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)

    load_job.result()

    destination_table = client.get_table(table_id)
    rows_after_insert = destination_table.num_rows
    print(f"rows after insert: {rows_after_insert}")


start = DummyOperator(task_id="start",dag=dag)
end = DummyOperator(task_id="end",dag=dag)

get_combined_records_task = BranchPythonOperator(
    task_id='get_combined_records_task',
    python_callable=get_combined_records,
    dag=dag
)

clean_and_process_records_task = PythonOperator(
    task_id='clean_and_process_records_task',
    python_callable=clean_and_process_records,
    dag=dag
)

upload_to_bigquery_task = PythonOperator(
    task_id='upload_to_bigquery',
    python_callable=upload_to_bigquery,
    dag=dag
)

start >> get_combined_records_task >> clean_and_process_records_task >> upload_to_bigquery_task >> end
get_combined_records_task >> end