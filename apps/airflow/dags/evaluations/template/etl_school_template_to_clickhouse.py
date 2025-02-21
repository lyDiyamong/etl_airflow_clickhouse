from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pymongo import MongoClient
import pandas as pd
import requests
import json
from datetime import datetime
import math
from dotenv import load_dotenv
import os
import logging

load_dotenv()

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

def clean_timestamps(data):
    """Clean timestamp fields to match ClickHouse DateTime format."""
    for row in data:
        for key, value in row.items():
            if isinstance(value, str) and 'T' in value and 'Z' in value:
                try:
                    dt = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
                    row[key] = dt.strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    try:
                        dt = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
                        row[key] = dt.strftime("%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        pass
    return data

def extract_data_from_mongodb(**kwargs):
    """Extract data from MongoDB dynamically."""
    client = MongoClient(os.getenv("MONGODB_URL"))
    db = client['evaluation-dev']
    collection = db['templates']
    
    # Fetch all documents
    cursor = collection.find({})
    data = list(cursor)
    
    client.close()
    
    # Convert to DataFrame for easier handling
    df = pd.DataFrame(data)
    
    # Convert timestamps in any datetime columns
    for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
        df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')
    
    return df.to_dict('records')

def transform_data(**kwargs):
    """Transform data with dynamic column handling."""
    data = kwargs['ti'].xcom_pull(task_ids='extract_data_from_mongodb')
    df = pd.DataFrame(data)
    
    # Any additional transformations can be applied here
    
    return df.to_dict('records')

def get_clickhouse_table_columns():
    """
    Query ClickHouse to retrieve the column names for the target table.
    Adjust the database and table names as needed.
    """
    clickhouse_url = f'{os.getenv("CLICKHOUSE_HOST")}:{os.getenv("CLICKHOUSE_PORT")}'
    # Adjust database name here if needed (here, we assume the database is 'clickhouse')
    query = "SELECT name FROM system.columns WHERE database = 'clickhouse' AND table = 'templates' FORMAT JSON"
    
    response = requests.post(
        url=clickhouse_url,
        data=query,
        headers={'Content-Type': 'text/plain'},
        auth=(os.getenv("CLICKHOUSE_USER"), os.getenv("CLICKHOUSE_PASSWORD"))
    )
    
    if response.status_code != 200:
        raise Exception(f"Failed to fetch table columns: {response.text}")
    
    # ClickHouse returns a JSON with a "data" key holding a list of dicts like [{"name": "col1"}, ...]
    result = response.json()
    columns = [item["name"] for item in result["data"]]
    logger.info(f"ClickHouse table columns: {columns}")
    return columns

def load_data_to_clickhouse(**kwargs):
    """Load data into ClickHouse using the table schema for column mapping."""
    data = kwargs['ti'].xcom_pull(task_ids='transform_data')
    if not data:
        logger.info("No data to load")
        return

    # Clean timestamp fields
    cleaned_data = clean_timestamps(data)
    
    # Query ClickHouse to get the list of columns for the table
    table_columns = get_clickhouse_table_columns()

    # Format column names for the insert statement
    column_names = ', '.join(f'"{col}"' for col in table_columns)
    logger.info(f"Using ClickHouse columns: {column_names}")
    
    # Function to format a single value for ClickHouse insertion
    def format_value(value):
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return 'Null'
        elif isinstance(value, str):
            # Escape single quotes by doubling them
            return f"'{value.replace('\'', '\'\'')}'"
        elif isinstance(value, (dict, list)):
            return f"'{json.dumps(value).replace('\'', '\'\'')}'"
        return str(value)

    # Prepare rows dynamically by mapping each table column to the corresponding extracted value (or Null)
    formatted_rows = []
    for row in cleaned_data:
        # For each table column, use the value from the row if available; otherwise, insert Null
        formatted_values = [format_value(row.get(col)) for col in table_columns]
        formatted_rows.append(f"({', '.join(formatted_values)})")

    # Construct the final INSERT query
    query = f"""
        INSERT INTO clickhouse.templates ({column_names})
        VALUES {', '.join(formatted_rows)}
    """
    logger.info(f"Constructed Query: {query}")

    # Send the query to ClickHouse via HTTP
    clickhouse_url = f'{os.getenv("CLICKHOUSE_HOST")}:{os.getenv("CLICKHOUSE_PORT")}'
    response = requests.post(
        url=clickhouse_url,
        data=query,
        headers={'Content-Type': 'text/plain'},
        auth=(os.getenv("CLICKHOUSE_USER"), os.getenv("CLICKHOUSE_PASSWORD"))
    )

    if response.status_code != 200:
        raise Exception(f"Failed to load data to ClickHouse: {response.text}")
    else:
        logger.info("Data loaded successfully")

# Define the DAG
dag = DAG(
    'templates_to_clickhouse',
    default_args=default_args,
    description='ETL pipeline from MongoDB to ClickHouse with dynamic column handling based on table schema',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['templates']
)

# Define tasks
extract_task = PythonOperator(
    task_id='extract_data_from_mongodb',
    python_callable=extract_data_from_mongodb,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data_to_clickhouse',
    python_callable=load_data_to_clickhouse,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task
