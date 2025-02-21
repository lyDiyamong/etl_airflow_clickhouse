from abc import ABC, abstractmethod
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pymongo import MongoClient
import pandas as pd
import requests
import json
from datetime import datetime
import math
from dotenv import load_dotenv
import os
import logging

load_dotenv()
logger = logging.getLogger(__name__)

class DataSource(ABC):
    """Abstract base class for data sources"""
    @abstractmethod
    def extract(self):
        pass

    @abstractmethod
    def close(self):
        pass

class DataTransformer(ABC):
    """Abstract base class for data transformers"""
    @abstractmethod
    def transform(self, data):
        pass

class DataLoader(ABC):
    """Abstract base class for data loaders"""
    @abstractmethod
    def load(self, data):
        pass

class MongoDBSource(DataSource):
    """MongoDB data source implementation"""
    def __init__(self, connection_url, database, collection):
        self.connection_url = connection_url
        self.database = database
        self.collection = collection
        self.client = None
        
    def connect(self):
        self.client = MongoClient(self.connection_url)
        return self.client[self.database][self.collection]
    
    def extract(self):
        try:
            collection = self.connect()
            cursor = collection.find({})
            data = list(cursor)
            df = pd.DataFrame(data)
            
            # Convert timestamps
            for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
                df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')
            
            return df.to_dict('records')
        except Exception as e:
            logger.error(f"Extraction error: {str(e)}")
            raise
            
    def close(self):
        if self.client:
            self.client.close()

class TemplateTransformer(DataTransformer):
    """Transformer for template data"""
    def transform(self, data):
        if not data:
            return []
        
        df = pd.DataFrame(data)
        # Add any specific transformations here
        return df.to_dict('records')

class ClickHouseLoader(DataLoader):
    """ClickHouse data loader implementation"""
    def __init__(self, host, port, user, password, database, table):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.table = table
        self.url = f'{self.host}:{self.port}'

    def get_table_columns(self):
        """Get columns from ClickHouse table"""
        query = f"SELECT name FROM system.columns WHERE database = '{self.database}' AND table = '{self.table}' FORMAT JSON"
        response = self._execute_query(query)
        result = response.json()
        return [item["name"] for item in result["data"]]

    def _execute_query(self, query):
        """Execute query against ClickHouse"""
        response = requests.post(
            url=self.url,
            data=query,
            headers={'Content-Type': 'text/plain'},
            auth=(self.user, self.password)
        )
        if response.status_code != 200:
            raise Exception(f"ClickHouse query failed: {response.text}")
        return response

    def _format_value(self, value):
        """Format value for ClickHouse insertion"""
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return 'Null'
        elif isinstance(value, str):
            return f"'{value.replace('\'', '\'\'')}'"
        elif isinstance(value, (dict, list)):
            return f"'{json.dumps(value).replace('\'', '\'\'')}'"
        return str(value)

    def _clean_timestamps(self, data):
        """Clean timestamp fields"""
        for row in data:
            for key, value in row.items():
                if isinstance(value, str) and 'T' in value and 'Z' in value:
                    try:
                        dt = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
                        row[key] = dt.strftime("%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        try:
                            dt = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
                            row[key] = dt.strftime("%Y-%m-%d %H:%M:%S")
                        except ValueError:
                            pass
        return data

    def load(self, data):
        """Load data into ClickHouse"""
        if not data:
            logger.info("No data to load")
            return

        cleaned_data = self._clean_timestamps(data)
        table_columns = self.get_table_columns()
        column_names = ', '.join(f'"{col}"' for col in table_columns)

        formatted_rows = []
        for row in cleaned_data:
            formatted_values = [self._format_value(row.get(col)) for col in table_columns]
            formatted_rows.append(f"({', '.join(formatted_values)})")

        query = f"""
            INSERT INTO {self.database}.{self.table} ({column_names})
            VALUES {', '.join(formatted_rows)}
        """

        self._execute_query(query)
        logger.info("Data loaded successfully")

class ETLPipeline:
    """ETL Pipeline orchestrator"""
    def __init__(self, extractor: DataSource, transformer: DataTransformer, loader: DataLoader):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def run(self):
        try:
            # Extract
            data = self.extractor.extract()
            logger.info("Data extracted successfully")

            # Transform
            transformed_data = self.transformer.transform(data)
            logger.info("Data transformed successfully")

            # Load
            self.loader.load(transformed_data)
            logger.info("ETL pipeline completed successfully")

        except Exception as e:
            logger.error(f"ETL pipeline failed: {str(e)}")
            raise
        finally:
            self.extractor.close()

def create_dag(
    dag_id,
    schedule,
    extractor: DataSource,
    transformer: DataTransformer,
    loader: DataLoader,
    default_args=None
):
    """DAG factory function"""
    if default_args is None:
        default_args = {
            'owner': 'airflow',
            'depends_on_past': False,
            'retries': 1,
        }

    dag = DAG(
        dag_id,
        default_args=default_args,
        schedule_interval=schedule,
        start_date=days_ago(1),
        catchup=False,
    )

    def run_etl(**context):
        pipeline = ETLPipeline(extractor, transformer, loader)
        pipeline.run()

    with dag:
        PythonOperator(
            task_id='run_etl',
            python_callable=run_etl,
            provide_context=True,
        )

    return dag

# Example usage:
if __name__ == "__main__":
    # Create components
    mongo_source = MongoDBSource(
        os.getenv("MONGODB_URL"),
        'evaluation-dev',
        'templates'
    )
    
    transformer = TemplateTransformer()
    
    clickhouse_loader = ClickHouseLoader(
        host=os.getenv("CLICKHOUSE_HOST"),
        port=os.getenv("CLICKHOUSE_PORT"),
        user=os.getenv("CLICKHOUSE_USER"),
        password=os.getenv("CLICKHOUSE_PASSWORD"),
        database='clickhouse',
        table='templates'
    )

    # Create DAG
    dag = create_dag(
        dag_id='templates_to_clickhouse',
        schedule='@daily',
        extractor=mongo_source,
        transformer=transformer,
        loader=clickhouse_loader
    )