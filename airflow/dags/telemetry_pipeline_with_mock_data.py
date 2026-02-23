"""
NovaTrack Pipeline with Mock Data Generator.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import random
import json

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def generate_mock_telemetry_data(**context):
    """
    Generate mock telemetry data for testing.
    Simulates API extraction without needing real API.
    """
    import psycopg2
    import os
    
    print("Generating mock telemetry data...")
    
    # Connect to database
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=os.getenv('POSTGRES_PORT', 5432),
        database=os.getenv('POSTGRES_DB', 'novatrack'),
        user=os.getenv('POSTGRES_USER', 'novatrack_user'),
        password=os.getenv('POSTGRES_PASSWORD'),
    )
    
    cursor = conn.cursor()
    
    # Generate 100 random events
    execution_date = context['execution_date']
    
    for i in range(100):
        event_id = f"evt_{execution_date.strftime('%Y%m%d')}_{i:04d}"
        device_id = f"device_{random.randint(1, 10):03d}"
        event_type = random.choice(['page_view', 'button_click', 'api_call', 'error'])
        
        # Random timestamp within the day
        timestamp = execution_date - timedelta(
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        
        # Generate JSON payload
        payload = {
            'device_model': random.choice(['iPhone 13', 'iPhone 14', 'Samsung S23', 'Pixel 7']),
            'os_version': random.choice(['iOS 17', 'iOS 16', 'Android 14', 'Android 13']),
            'app_version': '2.5.1',
            'user_id': f"user_{random.randint(1, 50):03d}",
            'session_id': f"session_{random.randint(1, 100):03d}",
            'country': random.choice(['Netherlands', 'Germany', 'France', 'Belgium']),
            'city': random.choice(['Amsterdam', 'Berlin', 'Paris', 'Brussels']),
            'metric_value': random.uniform(50, 200),
            'duration_ms': random.randint(100, 3000),
        }
        
        # Insert with UPSERT (prevents duplicates on re-runs)
        cursor.execute("""
            INSERT INTO telemetry_events 
            (event_id, device_id, event_type, timestamp, payload, source_api, ingestion_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                device_id = EXCLUDED.device_id,
                timestamp = EXCLUDED.timestamp,
                payload = EXCLUDED.payload
        """, (
            event_id,
            device_id,
            event_type,
            timestamp,
            json.dumps(payload),
            'mock_generator',
            datetime.now()
        ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Successfully generated 100 mock events for {execution_date.date()}")
    return 100


with DAG(
    'novatrack_pipeline_with_mock_data',
    default_args=default_args,
    description='Pipeline with mock data generation',
    schedule_interval='0 2 * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['novatrack', 'demo', 'mock'],
) as dag:

    # Task 1: Generate mock data
    generate_data = PythonOperator(
        task_id='generate_mock_data',
        python_callable=generate_mock_telemetry_data,
    )

    # Task 2: Run dbt models
    dbt_run = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /usr/app/dbt_project && dbt deps --profiles-dir . && dbt run --profiles-dir . --target prod',
    )

    # Task 3: Run dbt tests
    dbt_test = BashOperator(
        task_id='run_dbt_tests',
        bash_command='cd /usr/app/dbt_project && dbt test --profiles-dir . --target prod',
    )

    # Task 4: Generate documentation 
    dbt_docs = BashOperator(
        task_id='generate_dbt_docs',
        bash_command='cd /usr/app/dbt_project && dbt docs generate --profiles-dir . --target prod',
    )

    # Dependencies
    generate_data >> dbt_run >> dbt_test >> dbt_docs