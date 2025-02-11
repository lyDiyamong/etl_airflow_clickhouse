from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 1),
    'retries': 1,
}

with DAG(
    'example_dag',
    default_args=default_args,
    description='An example DAG',
    schedule_interval=None,
    catchup=False,
    tags=['general']
) as dag:

    def say_hello():
        print("Hello, Airflow!")

    hello_task = PythonOperator(
        task_id='say_hello',
        python_callable=say_hello,
        dag=dag,
    )
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    start >> hello_task >> end
