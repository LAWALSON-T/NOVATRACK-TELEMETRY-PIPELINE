from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'novatrack_test_dbt_only',
    default_args=default_args,
    description='Test DBT transformations only',
    schedule_interval=None,  # Manual trigger only
    start_date=days_ago(1),
    catchup=False,
    tags=['test', 'dbt'],
) as dag:

    # Run dbt models
    dbt_run = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /usr/app/dbt_project && dbt deps --profiles-dir . && dbt run --profiles-dir . --target prod',
    )

    # Run dbt tests
    dbt_test = BashOperator(
        task_id='run_dbt_tests',
        bash_command='cd /usr/app/dbt_project && dbt test --profiles-dir . --target prod',
    )

    dbt_run >> dbt_test
