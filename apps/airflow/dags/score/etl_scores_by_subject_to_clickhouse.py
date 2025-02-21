from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pymongo import MongoClient
import pandas as pd
import requests
import json
import logging
from datetime import datetime
from collections import defaultdict


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
def calculate_average_scores(evaluations, scores):
    """
    Calculate scores for evaluations of type 'Subject' and group by referenceId
    """
    # Step 1: Create a dictionary of scores for quick lookup
    score_dict = defaultdict(list)
    for score in scores:
        score_dict[score['evaluationId']].append(to_float(score['score']))
    # Step 2: Group evaluations by parentId (exclude parentId='na')
    evaluations_by_parent = defaultdict(list)
    for evaluation in evaluations:
        if evaluation.get('parentId') != 'na':
            evaluations_by_parent[evaluation.get('parentId')].append(evaluation)

    # Step 3: Recursive function to calculate scores
    def calculate_scores_recursively(evaluation):
        evaluation_id = evaluation['evaluationId']
        children = evaluations_by_parent.get(evaluation_id, [])

        if children:
            # Calculate the average score of child evaluations
            child_scores = [
                calculate_scores_recursively(child)
                for child in children
                if child.get('type') == 'Subject'
            ]
            if child_scores:
                return sum(child_scores) / len(child_scores)
            return None
        else:
            # Get scores directly from the score dictionary
            scores = score_dict.get(evaluation_id, [])
            clean_none_scores = [0 if item is None else item for item in scores]
            return sum(clean_none_scores) / len(clean_none_scores) if clean_none_scores else None

    # Step 4: Filter and calculate for evaluations with type='Subject'
    results = []
    for evaluation in evaluations:
        if evaluation.get('type') == 'subject' :
            avg_score = calculate_scores_recursively(evaluation)
            if avg_score is not None :
                print(f"average score of subject: {avg_score}")
                print(f"evaluation: {evaluation}")
                results.append({
                    'schoolId': evaluation['schoolId'],
                    'campusId': evaluation.get('campusId', None),
                    'groupStructureId': evaluation.get('groupStructureId', None),
                    'structurePath': evaluation.get('structurePath', None),
                    'parentId': evaluation['parentId'],
                    'evaluationId': evaluation['evaluationId'],
                    'score': avg_score,
                    'maxScore': evaluation['maxScore'],
                    'subjectId': evaluation['referenceId'],
                    'templateId': evaluation['templateId'],
                    'configGroupId': evaluation['configGroupId'],
                    'createdAt': evaluation['createdAt']
                })

    return results
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
        "_id": 0, "score": 1, "evaluationId": 1, "studentId": 1,
        "scorerId": 1, "markedAt": 1
        }))

    # Pass data to the next task
    kwargs['ti'].xcom_push(key='evaluations', value=evaluations)
    kwargs['ti'].xcom_push(key='scores', value=scores)

    client.close()

def transform_data(**kwargs):
    """Transform data to calculate average scores"""
    
    # Retrieve data from XCom
    evaluations = kwargs['ti'].xcom_pull(key='evaluations', task_ids='extract_data_from_mongodb')
    scores = kwargs['ti'].xcom_pull(key='scores', task_ids='extract_data_from_mongodb')
    # print(f"evaluations: { evaluations }")
    # print(f"scores: { scores }")

    # Transform the data
    transformed_data = calculate_average_scores(evaluations, scores)
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

    formatted_rows = []
    for row in data:
        formatted_row = {}
        # Convert all fields that are objects (e.g., dict) to JSON strings
        for key, value in row.items():
            if value is None:
                formatted_row[key] = 'NULL'  # Replace None with SQL NULL
            elif isinstance(value, str):
                # Wrap strings in single quotes for SQL insertion
                formatted_row[key] = f"'{value}'"
            else:
                # Leave other values (like numbers) as is
                formatted_row[key] = value
        formatted_rows.append(formatted_row)
    logger.info(data[0:10])
    query = '''
        INSERT INTO clickhouse.subject_score ("score", "maxScore", "evaluationId", "subjectId", "schoolId", "campusId",
         "groupStructureId", "structurePath", "templateId", "configGroupId" ) VALUES 
    '''
    rows = ",".join([
    f"""
        ({row['score']}, {row['maxScore']}, {row['evaluationId']}, {row['subjectId']}, {row['schoolId']},
        {row['campusId']}, {row['groupStructureId']}, {row['structurePath']}, {row['templateId']}, {row['configGroupId']})
    """ for row in formatted_rows
    ])
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
    'scores_by_subject_to_clickhouse',
    default_args=default_args,
    description='Extract score data from Evaluation Service MongoDB, transform it to score by subject, and load it into ClickHouse',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['evaluation', 'subject', 'score']
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
