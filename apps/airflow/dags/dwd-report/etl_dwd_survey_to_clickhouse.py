from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.models import Variable
import pandas as pd
import requests
import logging
import sys
import os
from datetime import datetime

# Add the parent directory to the path so we can import utilities
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# from utilities import update_etl_timestamp

from dotenv import load_dotenv
load_dotenv()
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# etl_dwd_report_last_run = update_etl_timestamp("etl_survey_last_run")

# Constants
QUESTION_ID= '507d5d04-4c97-4187-a6f7-f59e1241ecab'
PROFILE_KEY='0f161c83-9ffd-419a-a448-83a44346d4b3'

def extract_data_from_postgres(**kwargs):
    # last_run_timestamp = Variable.get("etl_dwd_report_last_run", default_var="1970-01-01T00:00:00")
    academic_postgres_hook = PostgresHook(postgres_conn_id='academic-local-staging')
    survey_postgres_hook = PostgresHook(postgres_conn_id='survey-local-staging')

    academic_connection = academic_postgres_hook.get_conn()
    survey_connection = survey_postgres_hook.get_conn()

    with survey_connection.cursor() as cursor:
        sql_all_structures = f'''
            SELECT 
                "userAnswers", "userId"
            FROM answers
            WHERE "questionId" = '{QUESTION_ID}'
          
            ORDER BY "updatedAt" DESC;
        '''

        #   --  AND "updatedAt" > '{last_run_timestamp}'
        cursor.execute(sql_all_structures)
        survey_answer_data = cursor.fetchall()
        survey_answer_columns = [desc[0] for desc in cursor.description]
        survey_answer_records = pd.DataFrame(survey_answer_data, columns=survey_answer_columns).to_dict('records')

    survey_connection.close()
    student_phone_numbers = [survey_answer['userId'] for survey_answer in survey_answer_records]
    # Get student details
    with academic_connection.cursor() as cursor:
        sql_student_details = f'''
            SELECT 
                "gender",
                "profile"->>'phone' as "phone",
                "profile"->> '{PROFILE_KEY}' as "role"
            FROM student
            WHERE "profile"->>'phone' IN ({', '.join("'" + phone + "'" for phone in student_phone_numbers)})
        '''
        cursor.execute(sql_student_details)
        student_details_data = cursor.fetchall()
        student_details_columns = [desc[0] for desc in cursor.description]
        student_details_records = pd.DataFrame(student_details_data, columns=student_details_columns).to_dict('records')

    academic_connection.close();


    kwargs['ti'].xcom_push(key="survey_answer_records", value=survey_answer_records)
    kwargs['ti'].xcom_push(key="student_details_records", value=student_details_records)

def transform_data(**kwargs):
    ti = kwargs['ti']
    survey_answer_records = ti.xcom_pull(key="survey_answer_records", task_ids='extract_data_from_postgres')
    student_details_records = ti.xcom_pull(key="student_details_records", task_ids='extract_data_from_postgres')

    # Create a mapping for roles
    role_mapping = {
        "student-សិស្ស-និស្សិត": "student",
        "professional-worker-អ្នកធ្វើការ": "professional",
        "other": "other"
    }

    # Create a mapping for gender
    gender_mapping = {
        "male-ប្រុស": "male",
        "female-ស្រី": "female",
        "other": "other"
    }

    # Initialize counters for each survey option
    survey_options = {
        "a. Apply CV": "Apply CV", 
        "b. Ask for or consult about job information": "Ask for or consult about job information", 
        " c. Ask for academic information": "Ask for academic information", 
        "d. Edit CV/Cover letter": "Edit CV/Cover letter", 
        "e. Create/prepare CV/Cover letter": "Create/prepare CV/Cover letter", 
        "f. Job announcement; g. Employee recruitment": "Job announcement; Employee recruitment", 
        "h. Others": "Others"
    }
    
    # Initialize results dictionary
    results = []
    students_number = 0
    unknown_users = 0
    
    for option in survey_options.keys():
        # Initialize counts for this option
        student_counts = {"male": 0, "female": 0, "other": 0, "total": 0}
        professional_counts = {"male": 0, "female": 0, "other": 0, "total": 0}
        other_counts = {"male": 0, "female": 0, "other": 0, "total": 0}
        
        # Process each survey answer
        for answer in survey_answer_records:
            user_id = answer['userId']
            # Find the corresponding student details
            student = next((s for s in student_details_records if s['phone'] == user_id), None)
            logger.warning(f"Student: {student}")
            students_number += 1
            
            # Check if userAnswers exists and has the expected structure
            if 'userAnswers' not in answer or not answer['userAnswers'] or 'answers' not in answer['userAnswers']:
                logger.warning(f"Invalid userAnswers format for user {user_id}: {answer.get('userAnswers')}")
                continue
                
            try:
                # Check if this option was selected in the user's answers
                for content_item in answer['userAnswers']['answers']:
                    if content_item['content'] == option:
                        if student:
                            # We found a matching student record
                            gender = gender_mapping.get(student['gender'], "other")
                            role = role_mapping.get(student['role'], "other")
                            
                            # Increment the appropriate counter
                            if role == "student":
                                student_counts[gender] += 1
                                student_counts["total"] += 1
                            elif role == "professional":
                                professional_counts[gender] += 1
                                professional_counts["total"] += 1
                            else:
                                other_counts[gender] += 1
                                other_counts["total"] += 1
                        else:
                            # No matching student record found - count in "other" category
                            unknown_users += 1
                            logger.warning(f"No student record found for user ID: {user_id}")
                            # Add to the "other" category with gender "other"
                            other_counts["other"] += 1
                            other_counts["total"] += 1
            except Exception as e:
                logger.error(f"Error processing answer for user {user_id}: {str(e)}")
                logger.error(f"Answer data: {answer}")
        
        # Add this option's results to the overall results
        results.append({
            "surveyQuestionId": QUESTION_ID,
            "surveyName": survey_options[option],
            "studentDetails": tuple(student_counts.values()),
            "professionalDetails": tuple(professional_counts.values()),
            "otherDetails": tuple(other_counts.values()),
            "updatedAt": datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        })
    
    logger.warning(f"Students number: {students_number}")
    logger.warning(f"Unknown users (no matching student record): {unknown_users}")
    
    # Push the transformed data to XCom
    kwargs['ti'].xcom_push(key='transformed_survey_data', value=results)

def load_data_to_clickhouse(**kwargs):
    """Load data into ClickHouse."""
    data = kwargs['ti'].xcom_pull(key='transformed_survey_data', task_ids='transform_data')
    
    if not data:
        logger.warning("No data to load into ClickHouse")
        return
    
    # Prepare the ClickHouse HTTP endpoint and query
    clickhouse_url = f'{os.getenv("CLICKHOUSE_HOST")}:{os.getenv("CLICKHOUSE_PORT")}'
    
    def format_value(value, key):
        """Format values for ClickHouse with proper type handling."""
        if value is None:
            return "NULL"  # Ensure None is converted to NULL

        # Handle tuple fields explicitly
        if key in ['studentDetails', 'professionalDetails', 'otherDetails']:
            if value is None or not isinstance(value, (tuple, list)):
                return "(0, 0, 0, 0)"  # Default tuple if value is missing
            # Format as a tuple with parentheses, not an array
            elements = [str(int(elem)) if elem is not None else "0" for elem in value]
            return f"({','.join(elements)})"

        elif isinstance(value, str):
            escaped_value = value.replace("'", "''")
            return f"'{escaped_value}'"
        
        return str(value)

    # Build the formatted row values
    formatted_rows = []
    table_keys = list(data[0].keys())

    for row in data:
        formatted_values = [format_value(row[key], key) for key in table_keys]
        formatted_rows.append(f"({','.join(formatted_values)})")

    # Construct the query
    query = f'INSERT INTO clickhouse.dwd_survey ({",".join(table_keys)}) VALUES '
    query += ",".join(formatted_rows)
    
    # Send the query using requests
    try:
        response = requests.post(
            url=clickhouse_url,
            data=query.encode('utf-8'),  # Encode to handle special characters
            headers={'Content-Type': 'text/plain'},
            auth=(os.getenv("CLICKHOUSE_USER"), os.getenv("CLICKHOUSE_PASSWORD"))
        )
        
        if response.status_code != 200:
            error_msg = f"Failed to load data to ClickHouse: {response.text}"
            logger.error(error_msg)
            raise Exception(error_msg)
        
        logger.info(f"Successfully loaded {len(data)} records into dwd_survey table")
    except Exception as e:
        logger.error(f"Error loading data to ClickHouse: {str(e)}")
        raise

# Define the DAG
dag = DAG(
    'dwd_survey_report_etl',
    default_args=default_args,
    description='Extract dwd survey, transform it into dwd survey report, and load into ClickHouse',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['survey', 'academic']
)

extract_data_from_postgres_task = PythonOperator(
    task_id='extract_data_from_postgres',
    python_callable=extract_data_from_postgres,
    dag=dag
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

# load_data_task = PythonOperator(
#     task_id='load_data_to_clickhouse',
#     python_callable=load_data_to_clickhouse,
#     provide_context=True,
#     dag=dag,
# )

extract_data_from_postgres_task >> transform_data_task 
# >> load_data_task