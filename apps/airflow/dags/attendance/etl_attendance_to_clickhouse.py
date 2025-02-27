from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from pymongo import MongoClient
import pandas as pd
import requests
import logging
from datetime import datetime
from collections import defaultdict
from dotenv import load_dotenv
from calendar import month_name

# Load environment variables from the .env file
load_dotenv()
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Extracting data from PostgreSQL
def extract_data_from_postgres(**kwargs):

    schedule_postgres_hook = PostgresHook(postgres_conn_id='schedule-local-staging')
    academic_postgres_hook = PostgresHook(postgres_conn_id='academic-local-staging')

    schedule_connection = schedule_postgres_hook.get_conn()
    academic_connection = academic_postgres_hook.get_conn()


    # Get structure data
    with academic_connection.cursor() as cursor:
        sql_student = f'''
            SELECT "studentId", "firstName", "lastName", "structureRecordId"
            FROM student
            LIMIT 100;
        '''
        logger.info(f"Student sql: {sql_student}")
        cursor.execute(sql_student)
        student_data = cursor.fetchall()
        student_columns = [desc[0] for desc in cursor.description]
        student_records = pd.DataFrame(student_data, columns=student_columns).to_dict('records')
    
    academic_connection.close()


    structure_record_ids = set(student.get("structureRecordId") for student in student_records if student.get('structureRecordId'))
    cleaned_structure_ids = {sid for sid in structure_record_ids if sid != "undefined"}

    with schedule_connection.cursor() as cursor:
        sql_event = f'''
            SELECT "eventId", "name", "date", "calendarId", "type"
            FROM event WHERE "calendarId" IN ({", ".join("'" + sid + "'" for sid in cleaned_structure_ids)}) 
            LIMIT 100;
        '''
        logger.info(f"Event sql: {sql_event}")
        cursor.execute(sql_event)
        event_data = cursor.fetchall()
        event_columns = [desc[0] for desc in cursor.description]
        event_records = pd.DataFrame(event_data, columns=event_columns).to_dict('records')

    with schedule_connection.cursor() as cursor:
        sql_participant = f'''
            SELECT "participantId", "eventId", "attendanceStatus", "attendanceMarkBy", "calendarId"
            FROM participant WHERE "calendarId" IN ({", ".join("'" + sid + "'" for sid in cleaned_structure_ids)})
            LIMIT 100;
        '''
        logger.info(f"Participant sql: {sql_participant}")
        cursor.execute(sql_participant)
        participant_data = cursor.fetchall()
        participant_columns = [desc[0] for desc in cursor.description]
        participant_records = pd.DataFrame(participant_data, columns=participant_columns).to_dict('records')
    
    
    schedule_connection.close()

    kwargs['ti'].xcom_push(key='participants', value=participant_records)
    kwargs['ti'].xcom_push(key='events', value=event_records)

def mergeData(participant_records, event_records):
    # Replace these with your actual `participant_records` and `event_records`.
    df_participants = pd.DataFrame(participant_records)  # columns: participantId, eventId, attendanceStatus, ...
    df_events = pd.DataFrame(event_records)              # columns: eventId, name, date (YYYY-MM-DD), ...

    # 1. Join participant_records to event_records on eventId
    df_merged = df_participants.merge(df_events, on='eventId', how='left')

    # 2. Parse the date column and extract the month name
    df_merged['date'] = pd.to_datetime(df_merged['date'], errors='coerce')
    df_merged['month'] = df_merged['date'].dt.month.apply(
    lambda m: month_name[int(m)] if pd.notna(m) else None)

    # 3. Map each attendanceStatus to a numeric count
    #    - P  -> Present
    #    - AP -> Absent with Permission
    #    - A  -> Absent without Permission
    def status_to_counts(status):
        if status == 'P':
            return pd.Series([1, 0, 0])  # (present, AP, A)
        elif status == 'AP':
            return pd.Series([0, 1, 0])
        elif status == 'A':
            return pd.Series([0, 0, 1])
        else:
            return pd.Series([0, 0, 0])

    df_merged[['present', 'absentWithPermission', 'absentWithoutPermission']] = (
        df_merged['attendanceStatus'].apply(status_to_counts)
    )

    # 4. Group by participantId and month, summing up each attendance category
    df_grouped = df_merged.groupby(['participantId', 'month'], dropna=False)[
        ['present', 'absentWithPermission', 'absentWithoutPermission']
    ].sum().reset_index()

    # 5. Convert the grouped DataFrame to a list of dictionaries (or keep it as a DataFrame)
    transformed_data = df_grouped.to_dict('records')

    return transformed_data

# Transform Data
def transform_data(**kwargs) :
    student_records = kwargs['ti'].xcom_pull(key='students', task_ids='extract_data_from_postgres')
    participant_records = kwargs['ti'].xcom_pull(key='participants' ,task_ids='extract_data_from_postgres')
    event_records = kwargs['ti'].xcom_pull(key='events', task_ids='extract_data_from_postgres')
    transformed_data = mergeData(participant_records, event_records)
    logger.info(f"Transform Data {transformed_data[10:30]}")
    logger.info(f"Data records count {len(transformed_data)}")

    kwargs['ti'].xcom_push(key="transformed_data", value=transformed_data)



# Define the DAG
dag = DAG(
    'student_attendance_etl',
    default_args=default_args,
    description='Extract score data, transform it into student attendance, and load into ClickHouse',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['schedule', 'academic']
)

extract_task_postgres = PythonOperator(
    task_id='extract_data_from_postgres',
    python_callable=extract_data_from_postgres,
    provide_context=True,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)
# Set task dependencies
extract_task_postgres >> transform_data_task