from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.models import Variable
import pandas as pd
import requests
import logging
# from utilities import update_etl_timestamp
from datetime import datetime

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

# etl_dwd_report_last_run = update_etl_timestamp("etl_dwd_report_last_run")

def extract_data_from_postgres(**kwargs):

    # last_run_timestamp = Variable.get("etl_dwd_report_last_run", default_var="1970-01-01T00:00:00")

    postgres_hook = PostgresHook(postgres_conn_id='academic-local-staging')
    connection = postgres_hook.get_conn()

    # Get ALL structure records for the school first (not just those linked to students)
    with connection.cursor() as cursor:
        sql_all_structures = f'''
            SELECT 
                parent."structureRecordId" AS parent_id,
                parent."name" AS parent_name,
                parent."code" AS parent_code,
                COUNT(child."structureRecordId") AS structure_count,
                cp."name" AS campus_name,
                cp."campusId" AS campus_id,
                s."name" AS school_name,
                s."schoolId" AS school_id
            FROM structure_record AS parent
            LEFT JOIN structure_record AS child
                ON child."parentRecordId" = parent."structureRecordId"
            JOIN campus AS cp
                ON parent."campusId" = cp."campusId"
            JOIN school AS s
                ON parent."schoolId" = s."schoolId"
            WHERE parent."schoolId" = '{SCHOOL_ID}'
                AND parent."parentRecordId" IS NULL  -- Only top-level structures
            GROUP BY parent."structureRecordId", parent."name", parent."code", 
                     cp."name", cp."campusId", s."name", s."schoolId"
            ORDER BY parent."code";
        '''
        cursor.execute(sql_all_structures)
        all_structure_data = cursor.fetchall()
        structure_columns = [desc[0] for desc in cursor.description]
        all_structure_records = pd.DataFrame(all_structure_data, columns=structure_columns).to_dict('records')
    
    # Get student data grouped by campus and parent code
    with connection.cursor() as cursor:
        sql_student = f'''
        SELECT 
            parent_code,
            campus_id,
            campus_name,
            "gender",
            "role",
            SUM(student_count) AS "totalCount"
        FROM (
            -- Inner query (updated to handle students without structureRecordId)
            SELECT 
                COALESCE(parent."code", child."code", 'No Structure') AS parent_code,
                cp."campusId" AS campus_id,
                cp."name" AS campus_name,
                stu."gender",
                stu.profile->>'{PROFILE_KEY}' AS role,
                COUNT(*) AS student_count
            FROM student AS stu
            LEFT JOIN structure_record AS child
                ON stu."structureRecordId" = child."structureRecordId"
            LEFT JOIN structure_record AS parent
                ON child."parentRecordId" = parent."structureRecordId"
            LEFT JOIN campus AS cp
                ON COALESCE(parent."campusId", child."campusId") = cp."campusId"
            WHERE stu."schoolId" = '{SCHOOL_ID}' 
            
            GROUP BY parent_code, campus_id, campus_name, stu."gender", role
        ) subquery
        GROUP BY parent_code, campus_id, campus_name, "gender", "role"
        ORDER BY parent_code, campus_id, "gender", "role";
        '''

        cursor.execute(sql_student)
        student_data = cursor.fetchall()
        student_columns = [desc[0] for desc in cursor.description]
        student_records = pd.DataFrame(student_data, columns=student_columns).to_dict('records')

    connection.close()
    
    # Pass ALL structure records, not just those linked to students
    kwargs['ti'].xcom_push(key='all_structure_records', value=all_structure_records)
    kwargs['ti'].xcom_push(key="student_groupby_events", value=student_records)

def transform_data(**kwargs):
    ti = kwargs['ti']
    all_structure_records = ti.xcom_pull(key="all_structure_records", task_ids='extract_data_from_postgres')
    student_groupby_records = ti.xcom_pull(key="student_groupby_events", task_ids='extract_data_from_postgres')

    # Define your mapping from parent's code to event category names.
    category_mapping = {
        SOFT_SKILLS_CODE: "Soft Skills Event",
        HARD_SKILLS_CODE: "Hard Skills Event",
        NETWORKING_CODE: "Networking Event",
        SOCIAL_EVENT_CODE: "Social Event",
        MENTORING_CODE: "Mentoring Session"
    }

    # Initialize event categories with default values
    event_categories = [
        "Soft Skills Event", "Hard Skills Event", "Networking Event", "Social Event", "Mentoring Session", "Others"
    ]
    
    # Structure to hold our data, organized by event category
    categories_data = {}
    
    for category_name in event_categories:
        categories_data[category_name] = {
            "student": {"male": 0, "female": 0, "other": 0, "total": 0},
            "professional": {"male": 0, "female": 0, "other": 0, "total": 0},
            "other": {"male": 0, "female": 0, "other": 0, "total": 0},
            "event_count_by_campus": {}  # Will hold campus_id -> count mappings
        }

    # First, count events by campus
    for structure in all_structure_records:
        parent_code = structure.get("parent_code", "").strip()
        campus_id = structure.get("campus_id")
        campus_name = structure.get("campus_name")
        structure_count = structure.get("structure_count", 0)
        
        # Map the code to a category
        category = category_mapping.get(parent_code, "Others")
        
        # Initialize the campus entry if it doesn't exist
        if campus_id not in categories_data[category]["event_count_by_campus"]:
            categories_data[category]["event_count_by_campus"][campus_id] = {
                "campusName": campus_name,
                "campusId": campus_id,
                "eventCount": 0
            }
        
        # Add the structure count to the campus total
        categories_data[category]["event_count_by_campus"][campus_id]["eventCount"] += structure_count

    # Now process student counts using the parent_code from the SQL result
    for student in student_groupby_records:
        parent_code = student.get("parent_code")
        campus_id = student.get("campus_id")
        
        # Map the parent's code to an event category
        category = category_mapping.get(parent_code, "Others")
        
        # Log when we default to "Others"
        if category == "Others":
            logger.warning(f"Parent code '{parent_code}' not found in category mapping. Defaulting to 'Others'.")

        role = student['role']
        gender = student["gender"]
        count = int(student["totalCount"])
        
        role_key = ("student" if role == "student-សិស្ស-និស្សិត"
                    else "professional" if role == "professional-worker-អ្នកធ្វើការ"
                    else "other")
        gender_key = ("male" if gender == "male-ប្រុស"
                    else "female" if gender == "female-ស្រី"
                    else "other")
        
        # Increment counts
        categories_data[category][role_key][gender_key] += count
        categories_data[category][role_key]["total"] += count

    # Prepare the final records
    transformed_records = []

    school_name = all_structure_records[0].get("school_name", "Digital Workforce Development") if all_structure_records else "Digital Workforce Development"
    school_id = all_structure_records[0].get("school_id", SCHOOL_ID) if all_structure_records else SCHOOL_ID

    for category_name, data in categories_data.items():
        # Convert the campus event counts to the array of tuples format expected by ClickHouse
        event_count_details = []
        for campus_data in data["event_count_by_campus"].values():
            event_count_details.append((
                campus_data["campusName"],
                campus_data["campusId"],
                campus_data["eventCount"]
            ))
        
        transformed_records.append({
            "schoolId": school_id,
            "schoolName": school_name,
            "eventName": category_name,
            "studentDetails": tuple(data["student"].values()),
            "professionalDetails": tuple(data["professional"].values()),
            "otherDetails": tuple(data["other"].values()),
            "eventCountDetails": event_count_details,
            "updatedAt": datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
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

        # Handle array of tuples for eventCountDetails
        if key == 'eventCountDetails':
            if not value:
                return "[]"  # Empty array if no values
                
            formatted_tuples = []
            for tup in value:
                if len(tup) != 3:
                    logger.warning(f"Invalid tuple in eventCountDetails: {tup}")
                    continue
                    
                # Format each element in the tuple according to its expected type
                campus_name = f"'{tup[0].replace('\'', '\'\'')}'" if tup[0] else "''"  # String
                campus_id = f"'{tup[1]}'" if tup[1] else "NULL"  # UUID
                event_count = str(int(tup[2])) if tup[2] is not None else "0"  # UInt64
                
                formatted_tuples.append(f"({campus_name}, {campus_id}, {event_count})")
                
            return f"[{','.join(formatted_tuples)}]"
                
        # Handle tuple fields explicitly
        elif key in ['studentDetails', 'professionalDetails', 'otherDetails']:
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
    query = f'INSERT INTO clickhouse.dwd_report_v4 ({",".join(table_keys)}) VALUES '
    query += ",".join(formatted_rows)
    
    # Log the query for debugging
    logger.debug(f"ClickHouse query: {query}")
    
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
        
        logger.info(f"Successfully loaded {len(data)} records into dwd_report_test table")
    except Exception as e:
        logger.error(f"Error loading data to ClickHouse: {str(e)}")
        raise

# Define the DAG
dag = DAG(
    'dwd_report_v4_etl',
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

# Update ETL Timestamp
# update_timestamp = PythonOperator(
#     task_id='update_etl_timestamp',
#     python_callable=etl_dwd_report_last_run,
#     dag=dag,
# )

extract_data_task >> transfom_data_task >> load_task 
# >> update_timestamp