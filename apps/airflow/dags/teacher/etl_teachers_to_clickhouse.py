import logging.config
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

from datetime import datetime
import pandas as pd
import logging
import os
from dotenv import load_dotenv
import sys

# Add the parent directory of 'dags' to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from utilities.clickhouse_utils import execute_clickhouse_query, format_value, optimize_table

# Load environment variables from the .env file
load_dotenv()
logger = logging.getLogger(__name__)
# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

def update_etl_timestamp(**kwargs):
    """Update the timestamp of the last successful ETL run."""
    current_time = datetime.now().isoformat()
    Variable.set("etl_teachers_last_run", current_time)
    logger.info(f"Updated ETL timestamp to: {current_time}")
    return current_time

def extract_teachers_from_postgres(**kwargs):
    """Extract data from PostgreSQL."""
    # Fetch the last ETL run timestamp (default: earliest date if not set)
    last_run_timestamp = Variable.get("etl_teachers_last_run", default_var="1970-01-01T00:00:00")
    
    logger.info(f"Extracting teachers updated after: {last_run_timestamp}")

    # The postgres_conn_id refers to the connection ID configured in Airflow
    postgres_hook = PostgresHook(postgres_conn_id='academic-local-staging')
    sql = f'''
        SELECT DISTINCT ON ("teacherId") 
        "teacherId", "schoolId", "campusId", "groupStructureId", "structureRecordId", 
        "subjectId", "employeeId", "firstName", "lastName", "firstNameNative", 
        "lastNameNative", "idCard", "gender", "email", "phone", 
        "position", "createdAt", "updatedAt", "department", "archiveStatus"
        FROM teacher
        WHERE "updatedAt" > '{last_run_timestamp}'
        ORDER BY "teacherId", "updatedAt" DESC;
    '''
    
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(data, columns=columns)

    # Convert timestamps to string format for JSON serialization
    for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
        df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')

    cursor.close()
    connection.close()
    
    records = df.to_dict('records')
    logger.info(f"Extracted {len(records)} updated teacher records")
    
    # Pass the data to the next task
    kwargs['ti'].xcom_push(key='extracted_teachers', value=records)
    
    return records

def load_teachers_to_clickhouse(**kwargs):
    """Load data into ClickHouse with proper string escaping."""
    data = kwargs['ti'].xcom_pull(task_ids='extract_teachers_from_postgres', key='extracted_teachers')
    
    if not data:
        logger.info("No updated teacher records to load")
        return "No records to load"

    # Format rows with proper type handling
    formatted_rows = []
    table_keys = [keys for keys in data[0].keys()]
    for row in data:
        formatted_values = [format_value(value, is_uuid=key.endswith('Id') and key != 'teacherId') 
                           for key, value in row.items()]
        formatted_rows.append(f"({','.join(formatted_values)})")

    # Prepare the query
    table_name = f'{os.getenv("CLICKHOUSE_DB")}.teacher_testing'
    query = f'''
            INSERT INTO {table_name}
            ({','.join(('"' + key + '"' for key in table_keys))}) 
            VALUES {','.join(formatted_rows)}
        '''

    # Execute the query
    try:
        success = execute_clickhouse_query(query, with_response=False)
        if success:
            logger.info(f"Successfully loaded {len(formatted_rows)} updated teacher records to ClickHouse")
            return f"Successfully loaded {len(formatted_rows)} rows to ClickHouse"
    except Exception as e:
        logger.error(f"Error loading data to ClickHouse: {str(e)}")
        raise

def optimize_clickhouse_table(**kwargs):
    """Force merge the ClickHouse table to remove duplicates."""
    table_name = "teacher_testing"
    
    try:
        success = optimize_table(table_name)
        if success:
            logger.info(f"Successfully optimized table {table_name}")
            return f"Successfully optimized table {table_name}"
    except Exception as e:
        logger.error(f"Error optimizing ClickHouse table: {str(e)}")
        raise

# Define the DAG
dag = DAG(
    'teachers_to_clickhouse',
    default_args=default_args,
    description='Copy teacher data from Academic Service Postgres to ClickHouse',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['academic', 'teacher']
)

# Define tasks
extract_task = PythonOperator(
    task_id='extract_teachers_from_postgres',
    python_callable=extract_teachers_from_postgres,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_teachers_to_clickhouse',
    python_callable=load_teachers_to_clickhouse,
    provide_context=True,
    dag=dag,
)

# Step 3: Update ETL Timestamp
update_timestamp = PythonOperator(
    task_id='update_etl_timestamp',
    python_callable=update_etl_timestamp
)

# Step 4: Optimize ClickHouse table
optimize_task = PythonOperator(
    task_id='optimize_clickhouse_table',
    python_callable=optimize_clickhouse_table,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_task >> load_task >> optimize_task >> update_timestamp