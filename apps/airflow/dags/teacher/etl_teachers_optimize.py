"""
This DAG handles the optimization of the teacher table in ClickHouse.
It's triggered by the main ETL DAG and runs the OPTIMIZE command.
"""
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import os
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

def optimize_clickhouse_table(**kwargs):
    """Force merge the ClickHouse table to remove duplicates."""
    clickhouse_url = f'{os.getenv("CLICKHOUSE_HOST")}:{os.getenv("CLICKHOUSE_PORT")}'
    table_name = f'{os.getenv("CLICKHOUSE_DB")}.teacher_testing'
    
    # Construct the OPTIMIZE query
    query = f'OPTIMIZE TABLE {table_name} FINAL'
    
    try:
        logger.info(f"Running OPTIMIZE on table {table_name}")
        response = requests.post(
            url=clickhouse_url,
            data=query,
            headers={'Content-Type': 'text/plain'},
            auth=(os.getenv("CLICKHOUSE_USER"), os.getenv("CLICKHOUSE_PASSWORD"))
        )
        
        if response.status_code != 200:
            error_msg = f"Failed to optimize ClickHouse table: {response.text}"
            logger.error(error_msg)
            raise Exception(error_msg)
        
        logger.info(f"Successfully optimized table {table_name}")
        return f"Successfully optimized table {table_name}"
    except Exception as e:
        logger.error(f"Error optimizing ClickHouse table: {str(e)}")
        raise

# Define the DAG
optimize_dag = DAG(
    'teachers_clickhouse_optimize',
    default_args=default_args,
    description='Optimize teacher table in ClickHouse',
    schedule_interval=None,  # This is an externally triggered DAG
    start_date=days_ago(1),
    catchup=False,
    tags=['academic', 'teacher', 'optimize']
)

optimize_task = PythonOperator(
    task_id='optimize_clickhouse_table',
    python_callable=optimize_clickhouse_table,
    provide_context=True,
    dag=optimize_dag,
) 