"""
Airflow DAG for NovaTrack Telemetry Pipeline.

This DAG runs daily and:
1. Extracts data from API
2. Loads into PostgreSQL
3. Transforms with dbt
4. Runs quality checks
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from ingestion import APIClient, DatabaseLoader, Config

# DAG default settings
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,  # Don't wait for previous runs
    'email': ['data-engineering@novatrack.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,  # Retry 3 times if task fails
    'retry_delay': timedelta(minutes=5),  # Wait 5 min between retries
}


def extract_telemetry_data(**context):
    """Extract data from REST API."""
    print("Starting data extraction...")
    
    # Initialize API client
    config = Config()
    api_client = APIClient(config)
    
    # Get yesterday's data
    execution_date = context['execution_date']
    start_date = execution_date - timedelta(days=1)
    end_date = execution_date
    
    print(f"Extracting data from {start_date} to {end_date}")
    
    # Fetch data
    data = api_client.fetch_telemetry_data(
        start_date=start_date,
        end_date=end_date,
        page_size=1000,
    )
    
    print(f"Extracted {len(data)} records")
    
    # Save data to pass to next task (XCom)
    context['task_instance'].xcom_push(key='telemetry_data', value=data)
    context['task_instance'].xcom_push(key='record_count', value=len(data))


def load_to_database(**context):
    """Load data into PostgreSQL."""
    print("Starting data load...")
    
    # Get data from previous task
    data = context['task_instance'].xcom_pull(
        task_ids='extract_telemetry_data',
        key='telemetry_data'
    )
    
    if not data:
        print("No data to load")
        return
    
    print(f"Loading {len(data)} records")
    
    # Load to database
    config = Config()
    db_loader = DatabaseLoader(config)
    
    loaded_count = db_loader.load_telemetry_data(
        data=data,
        table_name='telemetry_events',
    )
    
    print(f"Successfully loaded {loaded_count} records")
    
    db_loader.disconnect()


# Create the DAG
with DAG(
    'novatrack_telemetry_pipeline',
    default_args=default_args,
    description='ELT pipeline for NovaTrack telemetry data',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    start_date=days_ago(1),
    catchup=False,  # Don't run for past dates
    max_active_runs=1,  # Only one run at a time
    tags=['novatrack', 'telemetry', 'elt'],
) as dag:

    # Task 1: Extract from API
    extract_task = PythonOperator(
        task_id='extract_telemetry_data',
        python_callable=extract_telemetry_data,
        provide_context=True,
    )

    # Task 2: Load to database
    load_task = PythonOperator(
        task_id='load_to_database',
        python_callable=load_to_database,
        provide_context=True,
    )

    # Task 3: Run dbt models
    dbt_run_task = BashOperator(
        task_id='run_dbt_models',
        bash_command=(
            'cd /usr/app/dbt_project && '
            'dbt deps && '
            'dbt run --profiles-dir . --target prod'
        ),
    )

    # Task 4: Run dbt tests
    dbt_test_task = BashOperator(
        task_id='run_dbt_tests',
        bash_command=(
            'cd /usr/app/dbt_project && '
            'dbt test --profiles-dir . --target prod'
        ),
    )

    # Define task order (dependencies)
    extract_task >> load_task >> dbt_run_task >> dbt_test_task
