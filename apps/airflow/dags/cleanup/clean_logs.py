from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    'clean_airflow_logs',
    default_args=default_args,
    description='Delete old Airflow logs',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['general']
) as dag:

    clean_logs_task = BashOperator(
        task_id='clean_logs',
        bash_command="find /opt/airflow/logs -type f -mtime +1 -delete && find /opt/airflow/logs -type d -empty -delete",
    )
