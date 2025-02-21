import logging
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from .base import DataSource

logger = logging.getLogger(__name__)

class PostgresSource(DataSource):
    """PostgreSQL data source implementation using Airflow PostgresHook"""
    def __init__(self, conn_id, schema=None, sql=None, table=None):
        self.conn_id = conn_id
        self.schema = schema
        self.sql = sql
        self.table = table
        self.hook = None
        
        # Validate that either SQL or table is provided
        if not sql and not table:
            raise ValueError("Either SQL query or table name must be provided")
    
    def connect(self):
        self.hook = PostgresHook(postgres_conn_id=self.conn_id)
        return self.hook
    
    def extract(self):
        try:
            self.connect()
            
            # If SQL is provided, use it; otherwise, generate SELECT query from table
            query = self.sql
            if not query and self.table:
                schema_prefix = f"{self.schema}." if self.schema else ""
                query = f"SELECT * FROM {schema_prefix}{self.table}"
            
            # Execute query and get results as pandas DataFrame
            df = self.hook.get_pandas_df(query)
            
            # Convert timestamps to standard format
            for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
                df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')
            
            return df.to_dict('records')
            
        except Exception as e:
            logger.error(f"PostgreSQL extraction error: {str(e)}")
            raise
    
    def close(self):
        # PostgresHook manages connections through Airflow's connection pool
        # No explicit close needed, but we'll keep the method for consistency
        pass