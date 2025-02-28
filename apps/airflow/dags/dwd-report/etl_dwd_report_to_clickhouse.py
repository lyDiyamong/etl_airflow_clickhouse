from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from pymongo import MongoClient
import pandas as pd
import requests
import logging
from datetime import datetime
import uuid
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
# Constants for event categorization
SOFT_SKILLS_CODE = "SoftSkills"
HARD_SKILLS_CODE = "HardSkills"
NETWORKING_CODE="Networking"
SOCIAL_EVENT_CODE = "SocialEvent"
MENTORING_CODE = "Mentoring"


SCHOOL_ID = "0170ebdf-f7b9-4e6c-b803-2c1565677699"
# Define role in profile json 
PROFILE_KEY='0f161c83-9ffd-419a-a448-83a44346d4b3'

def extract_data_from_postgres(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id='academic-local-staging')
    connection = postgres_hook.get_conn()

    # Get structure data
    with connection.cursor() as cursor:
        sql_school = f'''
            SELECT "schoolId", "name", "code", "schoolType"
            FROM school
            WHERE "schoolId" = '0170ebdf-f7b9-4e6c-b803-2c1565677699'
        '''
        logger.info(f"Student sql: {sql_school}")
        cursor.execute(sql_school)
        school_data = cursor.fetchall()
        school_columns = [desc[0] for desc in cursor.description]
        school_records = pd.DataFrame(school_data, columns=school_columns).to_dict('records')

    # Get student data
    
    with connection.cursor() as cursor:
        sql_student = f'''
            SELECT 
                subquery."parentRecordId",
                subquery."gender",
                subquery."role",
                SUM(subquery.student_count) AS "totalCount"
            FROM (
                -- First, count students per structure record, grouped by gender and role
                SELECT 
                    stu."gender",
                    stu.profile->>'{PROFILE_KEY}' AS role,
                    sr."parentRecordId",
                    COUNT(*) AS student_count
                FROM student AS stu
                JOIN structure_record AS sr
                ON stu."structureRecordId" = sr."structureRecordId"
                WHERE stu."schoolId" = '{SCHOOL_ID}'
                GROUP BY stu."gender", role, sr."parentRecordId", sr."structureRecordId"
            ) AS subquery
            GROUP BY subquery."parentRecordId", subquery."gender", subquery."role";
        '''
        logger.info(f"Student sql: {sql_student}")
        cursor.execute(sql_student)
        student_data = cursor.fetchall()
        student_columns = [desc[0] for desc in cursor.description]
        student_records = pd.DataFrame(student_data, columns=student_columns).to_dict('records')
    logger.info(f'Student records {student_records}')
    event_recordIds = set(student.get("parentRecordId") for student in student_records)
    cleaned_parent_ids = {eid for eid in event_recordIds if eid is not None}
    logger.info(f"Clean {cleaned_parent_ids}")

    logger.info(f"event Record Ids {event_recordIds}")
    # Get structure data
    with connection.cursor() as cursor:
        sql_structure = f'''
            SELECT 
                sr."structureRecordId", 
                sr."name", 
                sr."parentRecordId",
                sr."code", 
                s."name" AS "schoolName", 
                s."code" AS "schoolCode"
            FROM structure_record AS sr
            JOIN school AS s
            ON sr."schoolId" = s."schoolId"
            WHERE sr."structureRecordId" IN ({", ".join("'" + eid + "'" for eid in cleaned_parent_ids)});
        '''
        logger.info(f"Student sql: {sql_structure}")
        cursor.execute(sql_structure)
        structure_data = cursor.fetchall()
        structure_columns = [desc[0] for desc in cursor.description]
        structure_record_records = pd.DataFrame(structure_data, columns=structure_columns).to_dict('records')
    connection.close()
    logger.info(f'strucuture record {structure_record_records}')
    # Pass transformed data to the next task
    kwargs['ti'].xcom_push(key='structure_records', value=structure_record_records)
    kwargs['ti'].xcom_push(key="student_groupby_events", value=student_records)

def transform_data(**kwargs):
    ti = kwargs['ti']
    structure_records = ti.xcom_pull(key="structure_records", task_ids='extract_data_from_postgres')
    student_groupby_records = ti.xcom_pull(key="student_groupby_events", task_ids='extract_data_from_postgres')

    # Initialize event categories with default values
    event_categories = [
        "Soft Skills Event", "Hard Skills Event", "Networking Event", "Social Event", "Mentoring Session", "Others"
    ]
    categories = {event: {"student": {"male": 0, "female": 0, "other": 0, "total": 0},
                           "professional": {"male": 0, "female": 0, "other": 0, "total": 0},
                           "other": {"male": 0, "female": 0, "other": 0, "total": 0},
                           "event_count": 0} for event in event_categories}

    # Process each structure record
    for structure in structure_records:
        event_code = structure.get("code", "").strip()
        event_name_raw = structure.get("name", "").strip()

        category_mapping = {
            SOFT_SKILLS_CODE: "Soft Skills Event",
            HARD_SKILLS_CODE: "Hard Skills Event",
            NETWORKING_CODE: "Networking Event",
            SOCIAL_EVENT_CODE: "Social Event",
            MENTORING_CODE : "Mentoring Event"
        }
        category = category_mapping.get(event_code, category_mapping.get(event_name_raw, "Others"))
        
        categories[category]["event_count"] += 1
        event_id = structure["structureRecordId"]
        
        for student in student_groupby_records:
            if student["parentRecordId"] in [event_id, None]:
                role, gender, count = student['role'], student["gender"], int(student["totalCount"])
                
                role_key = "student" if role == "student-សិស្ស-និស្សិត" else \
                           "professional" if role == "professional-worker-អ្នកធ្វើការ" else "other"
                
                gender_key = "male" if gender == "male-ប្រុស" else \
                             "female" if gender == "female-ស្រី" else "other"
                
                categories[category][role_key][gender_key] += count
                categories[category][role_key]["total"] += count
    
    school_name = structure_records[0]["schoolName"] if structure_records else "Digital Workforce Development"
    transformed_records = []

    for category_name, counts in categories.items():
        transformed_records.append({
            "schoolId": SCHOOL_ID,
            "schoolName": school_name,
            "eventName": category_name,
            "studentDetails": tuple(counts["student"].values()),
            "professionalDetails": tuple(counts["professional"].values()),
            "otherDetails": tuple(counts["other"].values()),
            "eventCount": counts["event_count"]
        })

    logger.info(f"Transformed records: {transformed_records}")
    kwargs["ti"].xcom_push(key='transformed_records', value=transformed_records)

def load_data_to_clickhouse(**kwargs):
    """Load data into ClickHouse."""
    data = kwargs['ti'].xcom_pull(key='transformed_records', task_ids='transform_data')
    
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
    query = f'INSERT INTO clickhouse.dwd_report ({",".join(table_keys)}) VALUES '
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
        
        logger.info(f"Successfully loaded {len(data)} records into dwd_report table")
    except Exception as e:
        logger.error(f"Error loading data to ClickHouse: {str(e)}")
        raise



# Define the DAG
dag = DAG(
    'dwd_report_transcript_etl',
    default_args=default_args,
    description='Extract dwd data, transform it into dwd report, and load into ClickHouse',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['survey', 'academic']
)

extract_data_task = PythonOperator(
    task_id='extract_data_from_postgres',
    python_callable=extract_data_from_postgres,
    provide_context=True,
    dag=dag,
)

transfom_data_task = PythonOperator(
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

extract_data_task >> transfom_data_task >> load_task