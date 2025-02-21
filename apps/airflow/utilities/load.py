import logging
import json
import math
import requests
from datetime import datetime
from .base import DataLoader

logger = logging.getLogger(__name__)

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