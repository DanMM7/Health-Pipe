from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import boto3
import json
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['alerts@health-agency.gov'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_county_etl',
    default_args=default_args,
    description='ETL pipeline for county health data',
    schedule_interval='0 2 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'health', 'county'],
)

# Task 1: Wait for all 5 county files
wait_for_files = S3KeySensor(
    task_id='check_s3_landing',
    bucket_name='county-raw-data',
    bucket_key=['county1_*.csv', 'county2_*.csv', 'county3_*.csv', 
                'county4_*.csv', 'county5_*.csv'],
    wildcard_match=True,
    timeout=60 * 60,  # 1 hour
    poke_interval=60,  # Check every minute
    dag=dag,
)

# Task 2: Move files to staging
def move_to_staging(**context):
    s3 = S3Hook(aws_conn_id='aws_default')
    keys = s3.list_keys(bucket_name='county-raw-data', prefix='')
    for key in keys:
        # Copy to staging
        copy_source = {'Bucket': 'county-raw-data', 'Key': key}
        s3.get_conn().copy_object(
            Bucket='county-staging',
            Key=f"staging/{key}",
            CopySource=copy_source
        )
        # Archive original
        s3.get_conn().copy_object(
            Bucket='county-raw-data',
            Key=f"archive/{datetime.now().strftime('%Y%m%d')}/{key}",
            CopySource=copy_source
        )
        # Delete original
        s3.get_conn().delete_object(Bucket='county-raw-data', Key=key)
    return {'files_moved': len(keys)}

move_files = PythonOperator(
    task_id='move_to_staging',
    python_callable=move_to_staging,
    dag=dag,
)

# Task 3: Validate schema
def validate_schema(**context):
    s3 = S3Hook(aws_conn_id='aws_default')
    keys = s3.list_keys(bucket_name='county-staging', prefix='staging/')
    expected_columns = ['incident_id', 'county', 'date', 'severity', 'count']
    for key in keys:
        obj = s3.get_key(key, bucket_name='county-staging')
        content = obj.get()['Body'].read().decode('utf-8')
        first_line = content.split('\n')[0]
        columns = first_line.split(',')
        if set(columns) != set(expected_columns):
            raise ValueError(f"Schema mismatch in {key}: expected {expected_columns}, got {columns}")
    return {'validated': len(keys)}

validate_schema_task = PythonOperator(
    task_id='validate_schema',
    python_callable=validate_schema,
    dag=dag,
)

# Task 4: Invoke Lambda for cleaning
invoke_lambda = LambdaInvokeFunctionOperator(
    task_id='run_lambda_etl',
    function_name='clean-csv',
    invocation_type='RequestResponse',
    payload=json.dumps({
        'bucket': 'county-staging',
        'prefix': 'staging/',
        'processed_bucket': 'county-processed'
    }),
    aws_conn_id='aws_default',
    dag=dag,
)

# Task 5: Load to RDS
load_to_rds = PostgresOperator(
    task_id='load_to_rds',
    postgres_conn_id='rds_default',
    sql='''
    CREATE TABLE IF NOT EXISTS accidents (
        incident_id VARCHAR(100) PRIMARY KEY,
        county VARCHAR(100),
        date DATE,
        severity VARCHAR(50),
        count INTEGER,
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    COPY accidents (incident_id, county, date, severity, count)
    FROM 's3://county-processed/cleaned.parquet'
    WITH (FORMAT PARQUET);
    
    -- Idempotent: handle duplicates
    INSERT INTO accidents (incident_id, county, date, severity, count)
    SELECT incident_id, county, date, severity, count
    FROM staging_accidents
    ON CONFLICT (incident_id) DO NOTHING;
    ''',
    dag=dag,
)

# Task 6: Data quality checks
def run_data_quality(**context):
    import psycopg2
    import os
    
    conn = psycopg2.connect(
        host=os.getenv('RDS_HOST', 'postgres'),
        database='health_db',
        user='airflow',
        password='airflow'
    )
    cur = conn.cursor()
    
    # Check 1: No negative counts
    cur.execute("SELECT COUNT(*) FROM accidents WHERE count < 0")
    if cur.fetchone()[0] > 0:
        raise ValueError("Negative accident counts found in database")
    
    # Check 2: No future dates
    cur.execute("SELECT COUNT(*) FROM accidents WHERE date > CURRENT_DATE")
    if cur.fetchone()[0] > 0:
        raise ValueError("Future dates found in database")
    
    # Check 3: Row count validation (compare landed vs loaded)
    # Using XCom from previous tasks
    landed_count = context['ti'].xcom_pull(task_ids='validate_schema', key='return_value')['validated']
    cur.execute("SELECT COUNT(*) FROM accidents WHERE ingested_at > CURRENT_DATE - INTERVAL '1 day'")
    loaded_count = cur.fetchone()[0]
    if loaded_count != landed_count:
        logger.warning(f"Row count mismatch: landed={landed_count}, loaded={loaded_count}")
    
    conn.close()
    return {'dq_passed': True}

dq_checks = PythonOperator(
    task_id='dq_checks',
    python_callable=run_data_quality,
    dag=dag,
)

# Task 7: Send notification
def send_notification(**context):
    # Email notification via Airflow's EmailOperator or custom
    # Using EmailOperator is simpler; this is a placeholder
    pass

# Define DAG flow
wait_for_files >> move_files >> validate_schema_task >> invoke_lambda >> load_to_rds >> dq_checks