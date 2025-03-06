from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import numpy as np
import requests
import logging
from datetime import datetime, timedelta
import uuid
from collections import defaultdict
import json

from dotenv import load_dotenv
import os
# Load environment variables from .env file
load_dotenv()
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def execute_clickhouse_query(query, with_response=True):
    """Execute a query against ClickHouse and optionally return the response."""
    clickhouse_url = f'{os.getenv("CLICKHOUSE_HOST")}:{os.getenv("CLICKHOUSE_PORT")}'
    
    try:
        if with_response:
            # For SELECT queries, use format in the query itself
            query_with_format = f"{query} FORMAT JSONEachRow"
            response = requests.post(
                url=clickhouse_url,
                data=query_with_format,
                headers={'Content-Type': 'text/plain'},
                auth=(os.getenv("CLICKHOUSE_USER"), os.getenv("CLICKHOUSE_PASSWORD"))
            )
            
            if response.status_code != 200:
                error_msg = f"Failed to execute ClickHouse query: {response.text}"
                logger.error(error_msg)
                raise Exception(error_msg)
            
            # Parse JSONEachRow format (one JSON object per line)
            results = []
            for line in response.text.strip().split('\n'):
                if line:  # Skip empty lines
                    try:
                        results.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse line: {line}, Error: {str(e)}")
                        continue
            return results
            
        else:
            # For INSERT/CREATE queries
            response = requests.post(
                url=clickhouse_url,
                data=query,
                headers={'Content-Type': 'text/plain'},
                auth=(os.getenv("CLICKHOUSE_USER"), os.getenv("CLICKHOUSE_PASSWORD"))
            )
            
            if response.status_code != 200:
                error_msg = f"Failed to execute ClickHouse query: {response.text}"
                logger.error(error_msg)
                raise Exception(error_msg)
            
            return True
        
    except Exception as e:
        logger.error(f"Error executing ClickHouse query: {str(e)}")
        raise

def extract_transcript_data(**kwargs):
    """Extract transcript data from ClickHouse."""
    # Query to extract transcript data
    query = """
    SELECT 
        structureRecordId,
        structureRecordName,
        subjectDetails,
        markedAt
    FROM 
        clickhouse.student_transcript_staging
    WHERE 
        markedAt >= dateAdd(month, -6, now())  -- Limit to recent records for efficiency
    """
    
    # Execute query and parse response
    result = execute_clickhouse_query(query)
    
    if not result:
        logger.warning("No transcript data retrieved from ClickHouse")
        return []
    
    # Pass data to the next task
    kwargs['ti'].xcom_push(key='transcript_data', value=result)
    logger.info(f"Extracted {len(result)} transcript records from ClickHouse")

def transform_to_class_scores(**kwargs):
    """Transform transcript data into class yearly scores."""
    # Retrieve transcript data from previous task
    transcript_data = kwargs['ti'].xcom_pull(key='transcript_data', task_ids='extract_transcript_data')
    
    if not transcript_data:
        logger.warning("No transcript data to transform")
        return []
    
    # Add debug logging
    logger.info(f"Sample transcript record: {transcript_data[0] if transcript_data else 'No data'}")
    
    # Group data by class (structureRecordId) and subject
    class_subject_data = defaultdict(lambda: defaultdict(list))
    
    # Process each transcript record
    for record in transcript_data:
        class_id = record.get('structureRecordId')
        class_name = record.get('structureRecordName')
        
        # Debug logging for subject details
        logger.info(f"Subject details type: {type(record.get('subjectDetails'))}")
        logger.info(f"Sample subject details: {record.get('subjectDetails')[:1] if record.get('subjectDetails') else 'No details'}")
        
        try:
            # If subjectDetails is a string, try to parse it
            subject_details = record.get('subjectDetails', [])
            if isinstance(subject_details, str):
                subject_details = json.loads(subject_details)
            
            # Process each subject detail
            for subject_detail in subject_details:
                try:
                    # Try accessing as dictionary first
                    if isinstance(subject_detail, dict):
                        subject_id = subject_detail.get('subjectEvaluationId')
                        subject_name = subject_detail.get('subjectName')
                        score = subject_detail.get('score')
                        percentage = subject_detail.get('percentage')
                        month_name = subject_detail.get('monthName')
                        month_eval_id = subject_detail.get('monthEvaluationId')
                        semester_name = subject_detail.get('semesterName')
                        semester_eval_id = subject_detail.get('semesterEvaluationId')
                    else:
                        # If it's a list/tuple, access by index
                        subject_id = subject_detail[0]
                        subject_name = subject_detail[1]
                        score = subject_detail[5]
                        percentage = subject_detail[7]
                        month_name = subject_detail[14]
                        month_eval_id = subject_detail[15]
                        semester_name = subject_detail[16]
                        semester_eval_id = subject_detail[17]
                    
                    # Skip if essential data is missing
                    if not all([class_id, subject_name]):
                        continue
                    
                    # Create a key for class+subject combination
                    key = (class_id, class_name, subject_name)
                    
                    # Add subject detail to the appropriate month group
                    if month_name and month_eval_id:
                        data_point = {
                            'score': float(score) if score is not None else 0,
                            'percentage': float(percentage) if percentage is not None else 0,
                            'month_name': month_name,
                            'month_eval_id': month_eval_id,
                            'semester_name': semester_name,
                            'semester_eval_id': semester_eval_id
                        }
                        class_subject_data[key]['monthly_data'].append(data_point)
                
                except (IndexError, KeyError, TypeError) as e:
                    logger.warning(f"Error processing subject detail: {subject_detail}, Error: {str(e)}")
                    continue
                
        except Exception as e:
            logger.error(f"Error processing record: {record}, Error: {str(e)}")
            continue
    
    # Process the grouped data to calculate aggregates
    class_yearly_scores = []
    
    for (class_id, class_name, subject_name), data in class_subject_data.items():
        monthly_data = data.get('monthly_data', [])
        
        if not monthly_data:
            continue
        
        # Group monthly data by month name/id
        month_groups = defaultdict(list)
        for point in monthly_data:
            key = (point['month_name'], point['month_eval_id'])
            month_groups[key].append(point)
        
        # Calculate monthly averages
        month_details = []
        for (month_name, month_eval_id), scores in month_groups.items():
            avg_score = sum(s['score'] for s in scores) / len(scores)
            # Find the semester this month belongs to (use the first entry's semester)
            semester_name = scores[0]['semester_name'] if scores else ''
            semester_id = scores[0]['semester_eval_id'] if scores else None
            
            month_details.append((month_name, round(avg_score, 2), semester_name, semester_id))
        
        # Group data by semester for semester calculations
        semester_groups = defaultdict(list)
        for point in monthly_data:
            if point['semester_name'] and point['semester_eval_id']:
                key = (point['semester_name'], point['semester_eval_id'])
                semester_groups[key].append(point)
        
        # Calculate semester details
        semester_details = []
        for (semester_name, semester_id), scores in semester_groups.items():
            avg_score = sum(s['score'] for s in scores) / len(scores)
            
            # Placeholder: In real implementation, extract real exam scores
            # For now, we'll simulate with a slight variation from the average
            exam_score = avg_score * (1 + (np.random.random() * 0.1 - 0.05))
            
            # Calculate final score (60% average + 40% exam)
            final_score = (avg_score * 0.6) + (exam_score * 0.4)
            
            semester_details.append((
                semester_id, 
                semester_name, 
                round(avg_score, 2), 
                round(exam_score, 2), 
                round(final_score, 2)
            ))
        
        # Calculate yearly score (average of all semester final scores)
        yearly_score = 0
        if semester_details:
            yearly_score = sum(s[4] for s in semester_details) / len(semester_details)
        
        # Create the class yearly score record
        class_score = {
            'classId': int(class_id) if class_id.isdigit() else hash(class_id) % 2**32,
            'className': class_name,
            'subjectName': subject_name,
            'monthDetails': month_details,
            'semesterDetails': semester_details,
            'yearly_score': round(yearly_score, 2),
            'reExam_score': None,
            'created_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'updatedAt': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        class_yearly_scores.append(class_score)
    
    # Pass the transformed data to the next task
    kwargs['ti'].xcom_push(key='class_yearly_scores', value=class_yearly_scores)
    logger.info(f"Transformed data into {len(class_yearly_scores)} class yearly score records")

def load_to_clickhouse(**kwargs):
    """Load the transformed data into ClickHouse."""
    # Retrieve the transformed data
    class_yearly_scores = kwargs['ti'].xcom_pull(key='class_yearly_scores', task_ids='transform_to_class_scores')
    
    if not class_yearly_scores:
        logger.warning("No class yearly scores to load into ClickHouse")
        return
    
    # Format data for ClickHouse insertion
    def format_value(value, key):
        """Format values for ClickHouse with proper type handling."""
        if value is None:
            return "NULL"
        
        if key in ['created_at', 'updatedAt']:
            return f"'{value}'"
        
        if key == 'monthDetails':
            formatted_tuples = []
            for tup in value:
                elements = []
                for i, elem in enumerate(tup):
                    if elem is None:
                        elements.append("NULL")
                    elif i == 3 and elem is not None:  # monthParentId (UUID)
                        elements.append(f"'{elem}'")
                    elif isinstance(elem, str):
                        escaped = elem.replace("'", "''")
                        elements.append(f"'{escaped}'")
                    else:
                        elements.append(str(elem))
                formatted_tuples.append(f"({','.join(elements)})")
            
            return f"[{','.join(formatted_tuples)}]"
        
        if key == 'semesterDetails':
            formatted_tuples = []
            for tup in value:
                elements = []
                for i, elem in enumerate(tup):
                    if elem is None:
                        elements.append("NULL")
                    elif i == 0 and elem is not None:  # semesterId (UUID)
                        elements.append(f"'{elem}'")
                    elif isinstance(elem, str):
                        escaped = elem.replace("'", "''")
                        elements.append(f"'{escaped}'")
                    else:
                        elements.append(str(elem))
                formatted_tuples.append(f"({','.join(elements)})")
            
            return f"[{','.join(formatted_tuples)}]"
        
        if isinstance(value, str):
            escaped_value = value.replace("'", "''")
            return f"'{escaped_value}'"
        
        return str(value)
    
    # Build the formatted row values
    formatted_rows = []
    table_keys = list(class_yearly_scores[0].keys())
    
    for row in class_yearly_scores:
        formatted_values = [format_value(row[key], key) for key in table_keys]
        formatted_rows.append(f"({','.join(formatted_values)})")
    
    # Construct the query
    query = f'INSERT INTO clickhouse.class_yearly_scores ({",".join(table_keys)}) VALUES '
    query += ",".join(formatted_rows)
    
    # Execute the query
    success = execute_clickhouse_query(query, with_response=False)
    
    if success:
        logger.info(f"Successfully loaded {len(class_yearly_scores)} records into class_yearly_scores table")
    else:
        logger.error("Failed to load data to class_yearly_scores table")

# Define the DAG
dag = DAG(
    'etl_semester_avg_to_clickhouse',
    default_args=default_args,
    description='Extract transcript data from ClickHouse, transform into class yearly scores, and load back to ClickHouse',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['academic', 'scores', 'transcript', 'clickhouse']
)

# Define tasks
extract_task = PythonOperator(
    task_id='extract_transcript_data',
    python_callable=extract_transcript_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_to_class_scores',
    python_callable=transform_to_class_scores,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_clickhouse',
    python_callable=load_to_clickhouse,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task