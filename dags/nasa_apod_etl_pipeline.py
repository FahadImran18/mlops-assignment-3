"""
NASA APOD ETL Pipeline DAG

This DAG orchestrates a complete ETL pipeline that:
1. Extracts data from NASA APOD API
2. Transforms the JSON response into a clean DataFrame
3. Loads data to PostgreSQL and CSV file
4. Versions the CSV file using DVC
5. Commits DVC metadata to Git
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Add the plugins directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'plugins'))

from nasa_apod_etl import extract_apod_data, transform_apod_data, load_to_postgres, load_to_csv, version_with_dvc, commit_to_git

# Get Airflow home directory (works for both local and Astronomer)
# Astronomer sets AIRFLOW_HOME=/usr/local/airflow, local uses /opt/airflow
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')

# Default arguments for the DAG
default_args = {
    'owner': 'mlops_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
with DAG(
    'nasa_apod_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for NASA APOD data with DVC versioning',
    schedule_interval=timedelta(days=1),  # Run daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['mlops', 'etl', 'nasa', 'dvc'],
) as dag:

    # Step 1: Data Extraction
    extract_task = PythonOperator(
        task_id='extract_apod_data',
        python_callable=extract_apod_data,
        op_kwargs={
            'api_key': 'DEMO_KEY',
            'output_path': '/tmp/apod_raw.json'
        }
    )

    # Step 2: Data Transformation
    transform_task = PythonOperator(
        task_id='transform_apod_data',
        python_callable=transform_apod_data,
        op_kwargs={
            'input_path': '/tmp/apod_raw.json',
            'output_path': '/tmp/apod_cleaned.json'
        }
    )

    # Step 3a: Load to PostgreSQL
    load_to_postgres_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        op_kwargs={
            'input_path': '/tmp/apod_cleaned.json',
            'postgres_conn_id': 'postgres_default'
        }
    )

    # Step 3b: Load to CSV
    load_to_csv_task = PythonOperator(
        task_id='load_to_csv',
        python_callable=load_to_csv,
        op_kwargs={
            'input_path': '/tmp/apod_cleaned.json',
            'output_path': f'{AIRFLOW_HOME}/data/apod_data.csv'
        }
    )

    # Step 4: Version with DVC
    version_dvc_task = PythonOperator(
        task_id='version_with_dvc',
        python_callable=version_with_dvc,
        op_kwargs={
            'csv_path': f'{AIRFLOW_HOME}/data/apod_data.csv',
            'dvc_repo_path': f'{AIRFLOW_HOME}/dvc_repo'
        }
    )

    # Step 5: Commit to Git
    commit_git_task = PythonOperator(
        task_id='commit_to_git',
        python_callable=commit_to_git,
        op_kwargs={
            'dvc_repo_path': f'{AIRFLOW_HOME}/dvc_repo',
            'commit_message': 'Update APOD data via Airflow pipeline'
        }
    )

    # Define task dependencies
    extract_task >> transform_task >> [load_to_postgres_task, load_to_csv_task] >> version_dvc_task >> commit_git_task

