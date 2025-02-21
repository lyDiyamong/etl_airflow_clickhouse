from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import requests
import json

from dotenv import load_dotenv
import os
# Load environment variables from the .env file
load_dotenv()

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

def update_etl_timestamp():
    Variable.set("etl_students_last_run", datetime.now().isoformat())
    # kwargs['ti'].xcom_push(key='last_run_timestamp', value=datetime.now().isoformat())

def extract_students_from_postgres():
    """Extract data from PostgreSQL."""
    # Fetch the last ETL run timestamp (default: earliest date if not set)
    last_run_timestamp = Variable.get("etl_students_last_run", default_var="1970-01-01T00:00:00")
    # last_run_timestamp = kwargs['ti'].xcom_pull(key='last_run_timestamp')
    # if not last_run_timestamp:
    #     last_run_timestamp = "1970-01-01T00:00:00"  # Default value

    postgres_hook = PostgresHook(postgres_conn_id='academic-local') #Postgress connection ID that need to be created in Clickhouse
    sql = f'''
        SELECT DISTINCT ON ("uniqueKey") 
        "firstName", "lastName", "firstNameNative", "lastNameNative", 
        "dob", "gender", "idCard", "program", "remark", "profile",
        "noAttendance", "status", "finalAcademicStatus", "enrolledAt", 
        "createdAt", "updatedAt", "uniqueKey", "schoolId" 
        FROM student
        WHERE "updatedAt" > '{last_run_timestamp}'
        ORDER BY "uniqueKey", "updatedAt" DESC;
    '''
    # print(f'sql: {sql}')
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
    return df.to_dict('records')

def load_students_to_clickhouse(**kwargs):
    """Load data into ClickHouse."""
    data = kwargs['ti'].xcom_pull(task_ids='extract_students_from_postgres')

    # Replace Python None with SQL NULL
    formatted_rows = []
    for row in data:
        formatted_row = []
        for key, value in row.items():
            if value is None:
                formatted_row.append('NULL')
            elif isinstance(value, str):
                if key == 'gender':     # Sometime, the gender input is "Male", "M", "male" ... so we need normalize them
                    gender = value.lower()
                    if gender in ['male', 'm']:
                       value = "male"
                    elif gender in ['female', 'f']:
                        value = "female" 
                formatted_row.append(f"'{value}'")
            elif isinstance(value, dict):
                if key == 'profile':
                    value.pop('profile', None) # Some profile object has redundant profile key, so 
                json_string = json.dumps(value).replace("'", "\\'")
                formatted_row.append(f"'{json_string}'")
            else:
                formatted_row.append(value)
        formatted_rows.append(f"({','.join(map(str, formatted_row))})")

    # print(f"formatted_rows: {formatted_rows[0]}")
    # Prepare the ClickHouse HTTP endpoint and query
    clickhouse_url = f'{os.getenv("CLICKHOUSE_HOST")}:{os.getenv("CLICKHOUSE_PORT")}'
    query = f'''
            INSERT INTO {os.getenv("CLICKHOUSE_DB")}.student 
            ("firstName", "lastName", "firstNameNative", "lastNameNative", 
            "dob", "gender", "idCard", "program", "remark", "profile",
            "noAttendance", "status", "finalAcademicStatus", "enrolledAt",
            "createdAt", "updatedAt", "uniqueKey", "schoolId") 
            VALUES {','.join(formatted_rows)}
        '''

    # Send the query using requests
    response = requests.post(
        url=clickhouse_url,
        data=query,
        headers={'Content-Type': 'text/plain'},
        auth=(os.getenv("CLICKHOUSE_USER"), os.getenv("CLICKHOUSE_PASSWORD"))
    )

    if response.status_code != 200:
        raise Exception(f"Failed to load data to ClickHouse: {response.text}")

# Define the DAG
dag = DAG(
    'students_to_clickhouse',
    default_args=default_args,
    description='Copy student data from Academic Service Postgres to ClickHouse',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['academic', 'student']
)

# Define tasks
extract_task = PythonOperator(
    task_id='extract_students_from_postgres',
    python_callable=extract_students_from_postgres,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_students_to_clickhouse',
    python_callable=load_students_to_clickhouse,
    provide_context=True,
    dag=dag,
)
# Step 3: Update ETL Timestamp
update_timestamp = PythonOperator(
    task_id='update_etl_timestamp',
    python_callable=update_etl_timestamp
)

# Set task dependencies
extract_task >> load_task >> update_timestamp