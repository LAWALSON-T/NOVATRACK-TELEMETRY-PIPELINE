"""
Simplified Airflow DAG for NovaTrack - DBT Only.

This DAG only runs dbt transformations (no API ingestion for now).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# DAG default settings
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-engineering@novatrack.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
with DAG(
    'novatrack_telemetry_pipeline',
    default_args=default_args,
    description='ELT pipeline for NovaTrack telemetry data (DBT only)',
    schedule_interval='0 2 * * *',  # Daily at 2 AM UTC
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['novatrack', 'telemetry', 'dbt'],
) as dag:

    # Task 1: Run dbt models
    dbt_run_task = BashOperator(
        task_id='run_dbt_models',
        bash_command=(
            'cd /usr/app/dbt_project && '
            'dbt deps --profiles-dir . && '
            'dbt run --profiles-dir . --target prod'
        ),
    )

    # Task 2: Run dbt tests
    dbt_test_task = BashOperator(
        task_id='run_dbt_tests',
        bash_command=(
            'cd /usr/app/dbt_project && '
            'dbt test --profiles-dir . --target prod'
        ),
    )

    # Task 3: Generate dbt documentation
    dbt_docs_task = BashOperator(
        task_id='generate_dbt_docs',
        bash_command=(
            'cd /usr/app/dbt_project && '
            'dbt docs generate --profiles-dir . --target prod'
        ),
    )

    # Define task dependencies
    dbt_run_task >> dbt_test_task >> dbt_docs_task
