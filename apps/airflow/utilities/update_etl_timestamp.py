from airflow.models import Variable
from datetime import datetime

def update_etl_timestamp(etl_last_run: str):
    Variable.set(etl_last_run, datetime.now().isoformat())
