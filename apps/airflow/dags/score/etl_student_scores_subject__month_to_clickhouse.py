from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from pymongo import MongoClient
import pandas as pd
import requests
import json
import logging
from datetime import datetime
from collections import defaultdict
import uuid


from dotenv import load_dotenv
import os
# Load environment variables from the .env file
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
                    # Convert ISO 8601 timestamp to ClickHouse-compatible format
                    dt = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
                    row[key] = dt.strftime("%Y-%m-%d %H:%M:%S")  # Remove milliseconds and Z
                except ValueError:
                    try:
                        # Handle case without milliseconds
                        dt = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
                        row[key] = dt.strftime("%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        # Skip if it's not a valid timestamp
                        pass
    return data
def to_float(value):
    if value is None:
        return None  # Handle NoneType explicitly
    try:
        # Convert to float if possible
        return float(value)
    except ValueError:
        # Handle cases where conversion is not possible
        return None
from collections import defaultdict

def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
def format_datetime(dt_str):
        if not dt_str:
            return None
        try:
            # Parse ISO format datetime and convert to ClickHouse format
            dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.%fZ")
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            try:
                # Try without milliseconds
                dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ")
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None

def calculate_monthly_subject_scores(evaluations, scores, students, structure_records):
    """
    Transform raw evaluation, score, student, and structure data into a flattened monthly score record per subject.
    
    Returns a list of dictionaries matching the flattened table:
        student_month_subject_score
    """
    # 1. Build dictionaries for quick lookup
    # Dictionary for month evaluations (type "month")
    month_evaluations = {
        eval_rec['evaluationId']: eval_rec 
        for eval_rec in evaluations 
        if eval_rec.get('type', '') == 'month'
    }
    # Dictionary for subject evaluations (type "subject")
    subject_evaluations = {
        eval_rec['evaluationId']: eval_rec 
        for eval_rec in evaluations 
        if eval_rec.get('type', '')== 'subject'
    }
    # Dictionary for student information keyed by studentId
    student_dict = {
        stu['studentId']: stu
        for stu in students
    }
    # Dictionary for structure records keyed by structureRecordId
    structure_dict = {
        s['structureRecordId']: s
        for s in structure_records
    }

    # 2. Group score records by (subject evaluation, student)
    grouped_scores = defaultdict(list)
    for score in scores:
        # Consider only scores that belong to a subject evaluation
        subj_eval = subject_evaluations.get(score['evaluationId'])
        if subj_eval:
            key = (score['evaluationId'], score['studentId'])
            grouped_scores[key].append(score)
    logger.info(f'Group score : {grouped_scores}' )
    results = []
    # 3. Process each group to build the flattened record
    for (subject_eval_id, student_id), score_list in grouped_scores.items():
        # Calculate the average score if there are multiple records
        score_values = [to_float(s.get('score')) for s in score_list if s.get('score') is not None]
        if not score_values:
            continue
        total_score = sum(score_values) 
        
        # Get subject evaluation record
        subject_eval = subject_evaluations.get(subject_eval_id)
        if not subject_eval:
            continue
        
        # Get month evaluation record via the parentId of subject evaluation
        month_eval_id = subject_eval.get('parentId')
        month_eval = month_evaluations.get(month_eval_id, {})
        
        # Get student info record
        student_info = student_dict.get(student_id, {})

        # For structure info, extract the structure record id from the score's structurePath.
        # Assumes structurePath is formatted like "#<structureRecordId>" or contains it in a predictable position.
        structure_record_id = None
        first_score = score_list[0]
        if first_score.get('structurePath'):
            # For example, if structurePath is "#283990ce-20c6-45a0-bc39-9b088ff36b58"
            parts = first_score['structurePath'].split("#")
            if len(parts) > 1:
                structure_record_id = parts[1]
        structure_info = structure_dict.get(structure_record_id, {})

        # 4. Build the flattened record that matches the target table
        record = {
            # School & Campus info (from subject evaluation or fallback to month)
            'schoolId': subject_eval.get('schoolId'),
            'campusId': subject_eval.get('campusId') or month_eval.get('campusId'),
            
            # Structure / Class Info
            'structureRecordId': structure_info.get('structureRecordId'),
            'structureRecordName': structure_info.get('name'),
            'groupStructureId': subject_eval.get('groupStructureId') or student_info.get('groupStructureId') or structure_info.get('groupStructureId'),
            'structurePath': first_score.get('structurePath'),
            
            # Student Info
            'studentId': student_id,
            'studentFirstName': student_info.get('firstName'),
            'studentLastName': student_info.get('lastName'),
            'studentFirstNameNative': student_info.get('firstNameNative'),
            'studentLastNameNative': student_info.get('lastNameNative'),
            'idCard': first_score.get('idCard'),
            
            # Month Evaluation (Parent)
            'monthEvaluationId': month_eval.get('evaluationId'),
            'monthName': month_eval.get('name'),
            # Assuming attendanceColumn exists in month evaluation
            'monthStartDate': format_datetime(month_eval.get('attendanceColumn', {}).get('startDate')),
            'monthEndDate': format_datetime(month_eval.get('attendanceColumn', {}).get('endDate')),
            
            # Subject Evaluation (Child)
            'subjectEvaluationId': subject_eval.get('evaluationId'),
            'subjectName': subject_eval.get('name'),
            'subjectMaxScore': to_float(subject_eval.get('maxScore')),
            
            # Score Info
            'score': total_score,
            'scorerId': first_score.get('scorerId'),
            'markedAt': format_datetime(first_score.get('markedAt')),
            'description': first_score.get('description'),
            
            # Timestamp (using subject evaluation creation time as a reference)
            'createdAt': format_datetime(subject_eval.get('createdAt'))
        }
        results.append(record)
    logger.info(f"Result: {results}")
    return results

# Extracting data
# Extracting from mongo
def extract_data_from_mongodb(**kwargs):
    """Extract evaluations and score data from MongoDB."""
    # Connect to MongoDB
    client = MongoClient(os.getenv("MONGODB_URL"))
    db = client[f'{os.getenv("DB_EVALUATION")}-{os.getenv("ENVIRONMENT")}']  # Replace with your MongoDB database name
    # collection = db['applicants']  # collection name
    
    # Get all evaluations
    evaluations =  list(db['evaluations'].find({}, { 
        "_id": 0, "name": 1, "description": 1, "sort": 1, "maxScore": 1, 
        "coe": 1, "type": 1, "parentId": 1, "schoolId": 1, "campusId": 1,
        "groupStructureId": 1, "structurePath": 1, "evaluationId": 1, "templateId":1, 
        "configGroupId": 1, "referenceId": 1, "createdAt": 1
        }))
    scores =  list(db['scores'].find({}, { 
        "_id": 0, "score": 1, "evaluationId": 1, "studentId": 1, "idCard" : 1,
        "scorerId": 1, "markedAt": 1, "structurePath" : 1
        }))

    # Pass data to the next task
    kwargs['ti'].xcom_push(key='evaluations', value=evaluations)
    kwargs['ti'].xcom_push(key='scores', value=scores)

    client.close()
# Extracting from postgres
def extract_data_from_postgres(**kwargs):
    scores = kwargs['ti'].xcom_pull(key='scores', task_ids='extract_data_from_mongodb')
    logger.info(scores)

    # Extract the IDs from the score records
    structure_ids = set(score['structurePath'].split("#")[1] for score in scores if score.get('structurePath'))
    student_ids = set(score['studentId'] for score in scores if score.get('studentId'))
    logger.info(f"Student IDs: {student_ids}")

    postgres_hook = PostgresHook(postgres_conn_id='academic-local')
    connection = postgres_hook.get_conn()

    # Use a context manager for the first query (structure data)
    with connection.cursor() as cursor:
        sql_structure = f'''
            SELECT "structureRecordId", "name", "groupStructureId"
            FROM structure_record
            WHERE "structureRecordId" IN ({", ".join("'" + sid + "'" for sid in structure_ids)})
        '''
        cursor.execute(sql_structure)
        structure_data = cursor.fetchall()
        structure_columns = [desc[0] for desc in cursor.description]
        structure_record_records = pd.DataFrame(structure_data, columns=structure_columns).to_dict('records')
        logger.info(f"Structure data: {structure_record_records}")

    # Use a separate cursor for the second query (student data)
    with connection.cursor() as cursor:
        sql_student = f'''
            SELECT "studentId", "firstName", "lastName", "firstNameNative", "lastNameNative"
            FROM student
            WHERE "studentId" IN ({", ".join("'" + stid + "'" for stid in student_ids)})
        '''
        cursor.execute(sql_student)
        student_data = cursor.fetchall()
        student_columns = [desc[0] for desc in cursor.description]
        student_records = pd.DataFrame(student_data, columns=student_columns).to_dict('records')
        logger.info(f"Student data: {student_records}")

    connection.close()

    # Push the extracted data to XCom
    kwargs['ti'].xcom_push(key='structure_records', value=structure_record_records)
    kwargs['ti'].xcom_push(key='students', value=student_records)


def transform_data(**kwargs):
    """Transform data to calculate average scores"""
    
    # Retrieve data from XCom
    evaluations = kwargs['ti'].xcom_pull(key='evaluations', task_ids='extract_data_from_mongodb')
    scores = kwargs['ti'].xcom_pull(key='scores', task_ids='extract_data_from_mongodb')
    student = kwargs['ti'].xcom_pull(key = "students", task_ids='extract_data_from_postgres')
    structure_record = kwargs['ti'].xcom_pull(key = "structure_records", task_ids='extract_data_from_postgres')

    # print(f"evaluations: { evaluations }")
    # print(f"scores: { scores }")

    # Transform the data
    transformed_data = calculate_monthly_subject_scores(evaluations, scores, student, structure_record)
    # print(f"transformed data length: { len(transformed_data) }")
    # print(f"transformed data: { transformed_data }")
    # Pass transformed data to the next task
    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_data)

def load_data_to_clickhouse(**kwargs):
    """Load data into ClickHouse."""
    data = kwargs['ti'].xcom_pull(key='transformed_data', task_ids='transform_data')

    # Clean timestamp fields in the data
    # cleaned_data = clean_timestamps(data)
    
    # Prepare the ClickHouse HTTP endpoint and query
    clickhouse_url = f'{os.getenv("CLICKHOUSE_HOST")}:{os.getenv("CLICKHOUSE_PORT")}'

    def format_value(value):
        """Format values with proper type handling."""
        if value is None:
            return 'NULL'
        elif isinstance(value, str):
            # Try to determine if the string is a UUID
            try:
                uuid.UUID(value)
                return f"toUUID('{value}')"
            except ValueError:
                # Escape single quotes for regular strings
                escaped_value = value.replace("'", "\\'")
                return f"'{escaped_value}'"
        else:
            return str(value)

    # Format rows with proper type handling
    formatted_rows = []
    for row in data:
        formatted_values = [format_value(value) for value in row.values()]
        formatted_rows.append(f"({','.join(formatted_values)})")


    logger.info(data[0:10])
    query = '''
        INSERT INTO clickhouse.student_month_subject_score (
            -- School & Campus
            schoolId, campusId,
            -- Structure / Class Info
            structureRecordId, structureRecordName, groupStructureId, structurePath,
            -- Student Info
            studentId, studentFirstName, studentLastName, 
            studentFirstNameNative, studentLastNameNative, idCard,
            -- Month Evaluation
            monthEvaluationId, monthName, monthStartDate, monthEndDate,
            -- Subject Evaluation
            subjectEvaluationId, subjectName, subjectMaxScore,
            -- Score Info
            score, scorerId, markedAt, description,
            -- Timestamp
            createdAt
        ) VALUES 
    '''

    logger.info(f'Row {formatted_rows}')
    rows = ",".join(formatted_rows)
    query += rows
    

    # Log the query before sending it to ClickHouse
    # print(f"Query to ClickHouse: {query}")    
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
    'scores_by_subject_student_month_to_clickhouse',
    default_args=default_args,
    description='Extract score data from Evaluation Service MongoDB and extract student info from Academic Service Postgres, transform it to score by subject, and load it into ClickHouse',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['evaluation', 'subject', 'score', 'academic']
)

# Define tasks
extract_task_mongo = PythonOperator(
    task_id='extract_data_from_mongodb',
    python_callable=extract_data_from_mongodb,
    provide_context=True,
    dag=dag,
)

extract_task_postgres = PythonOperator(
    task_id='extract_data_from_postgres',
    python_callable=extract_data_from_postgres,
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
extract_task_mongo >> extract_task_postgres >> transform_task >> load_task
#  
