"""
Utility functions for interacting with ClickHouse database.
This module provides reusable functions for executing queries, formatting data,
and performing common operations like table optimization.
"""
import logging
import requests
import json
import os
import math
from typing import List, Dict, Any, Union, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

def execute_clickhouse_query(
    query: str, 
    with_response: bool = True, 
    host: Optional[str] = None,
    port: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None
) -> Union[List[Dict[str, Any]], bool]:
    """
    Execute a query against ClickHouse and optionally return the response.
    
    Args:
        query: The SQL query to execute
        with_response: Whether to return the query results (True) or just success status (False)
        host: ClickHouse host (defaults to CLICKHOUSE_HOST env var)
        port: ClickHouse port (defaults to CLICKHOUSE_PORT env var)
        user: ClickHouse user (defaults to CLICKHOUSE_USER env var)
        password: ClickHouse password (defaults to CLICKHOUSE_PASSWORD env var)
        
    Returns:
        For SELECT queries (with_response=True): List of dictionaries containing the results
        For other queries (with_response=False): Boolean indicating success
        
    Raises:
        Exception: If the query fails to execute
    """
    # Use provided credentials or fall back to environment variables
    host = host or os.getenv("CLICKHOUSE_HOST")
    port = port or os.getenv("CLICKHOUSE_PORT")
    user = user or os.getenv("CLICKHOUSE_USER")
    password = password or os.getenv("CLICKHOUSE_PASSWORD")
    
    clickhouse_url = f'{host}:{port}'
    
    try:
        if with_response:
            # For SELECT queries, use format in the query itself
            query_with_format = f"{query} FORMAT JSONEachRow"
            response = requests.post(
                url=clickhouse_url,
                data=query_with_format,
                headers={'Content-Type': 'text/plain'},
                auth=(user, password)
            )
            
            if response.status_code != 200:
                error_msg = f"Failed to execute ClickHouse query: {response.text}"
                logger.error(error_msg)
                raise Exception(error_msg)
            
            # Parse JSONEachRow format (one JSON object per line)
            results = []
            for line in response.text.strip().split('\n'):
                if line:  # Skip empty lines
                    try:
                        results.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse line: {line}, Error: {str(e)}")
                        continue
            return results
            
        else:
            # For INSERT/CREATE/OPTIMIZE queries
            response = requests.post(
                url=clickhouse_url,
                data=query,
                headers={'Content-Type': 'text/plain'},
                auth=(user, password)
            )
            
            if response.status_code != 200:
                error_msg = f"Failed to execute ClickHouse query: {response.text}"
                logger.error(error_msg)
                raise Exception(error_msg)
            
            return True
        
    except Exception as e:
        logger.error(f"Error executing ClickHouse query: {str(e)}")
        raise

def format_value(value: Any, is_uuid: bool = False) -> str:
    """
    Format a value for ClickHouse insertion with proper type handling.
    
    Args:
        value: The value to format
        is_uuid: Whether the value should be treated as a UUID
        
    Returns:
        Formatted value as a string
    """
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "NULL"
    
    if is_uuid and isinstance(value, str):
        return f"toUUID('{value}')"
    
    if isinstance(value, str):
        # Escape single quotes for regular strings
        escaped_value = value.replace("'", "''")
        return f"'{escaped_value}'"
    
    if isinstance(value, (dict, list)):
        # Convert dict/list to JSON string and escape
        json_str = json.dumps(value).replace("'", "''")
        return f"'{json_str}'"
    
    # For numeric types, booleans, etc.
    return str(value)

def format_date_value(date_str: Optional[str]) -> str:
    """
    Format a date string for ClickHouse.
    
    Args:
        date_str: Date string in various formats
        
    Returns:
        Formatted date string or NULL
    """
    if not date_str or date_str == "":
        return "NULL"
    
    try:
        # Try to parse and format the date
        if 'T' in date_str and 'Z' in date_str:
            try:
                dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%fZ")
            except ValueError:
                dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
            return f"'{dt.strftime('%Y-%m-%d %H:%M:%S')}'"
        else:
            # Assume it's already in a valid format
            return f"'{date_str}'"
    except Exception:
        # If parsing fails, return NULL
        return "NULL"

def optimize_table(
    table_name: str, 
    database: Optional[str] = None,
    final: bool = True,
    host: Optional[str] = None,
    port: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None
) -> bool:
    """
    Optimize a ClickHouse table to force merges and remove duplicates.
    
    Args:
        table_name: Name of the table to optimize
        database: Database name (defaults to CLICKHOUSE_DB env var)
        final: Whether to use FINAL keyword for complete optimization
        host: ClickHouse host (defaults to CLICKHOUSE_HOST env var)
        port: ClickHouse port (defaults to CLICKHOUSE_PORT env var)
        user: ClickHouse user (defaults to CLICKHOUSE_USER env var)
        password: ClickHouse password (defaults to CLICKHOUSE_PASSWORD env var)
        
    Returns:
        Boolean indicating success
        
    Raises:
        Exception: If the optimization fails
    """
    database = database or os.getenv("CLICKHOUSE_DB", "default")
    final_keyword = "FINAL" if final else ""
    
    query = f"OPTIMIZE TABLE {database}.{table_name} {final_keyword}"
    
    logger.info(f"Running OPTIMIZE on table {database}.{table_name}")
    return execute_clickhouse_query(
        query=query, 
        with_response=False,
        host=host,
        port=port,
        user=user,
        password=password
    )

def get_table_columns(
    table_name: str, 
    database: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None
) -> List[str]:
    """
    Get the column names for a ClickHouse table.
    
    Args:
        table_name: Name of the table
        database: Database name (defaults to CLICKHOUSE_DB env var)
        host: ClickHouse host (defaults to CLICKHOUSE_HOST env var)
        port: ClickHouse port (defaults to CLICKHOUSE_PORT env var)
        user: ClickHouse user (defaults to CLICKHOUSE_USER env var)
        password: ClickHouse password (defaults to CLICKHOUSE_PASSWORD env var)
        
    Returns:
        List of column names
        
    Raises:
        Exception: If the query fails
    """
    database = database or os.getenv("CLICKHOUSE_DB", "default")
    
    query = f"SELECT name FROM system.columns WHERE database = '{database}' AND table = '{table_name}'"
    
    result = execute_clickhouse_query(
        query=query,
        with_response=True,
        host=host,
        port=port,
        user=user,
        password=password
    )
    
    return [item["name"] for item in result]

def insert_data(
    table_name: str,
    data: List[Dict[str, Any]],
    database: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None
) -> bool:
    """
    Insert data into a ClickHouse table.
    
    Args:
        table_name: Name of the table
        data: List of dictionaries containing the data to insert
        database: Database name (defaults to CLICKHOUSE_DB env var)
        host: ClickHouse host (defaults to CLICKHOUSE_HOST env var)
        port: ClickHouse port (defaults to CLICKHOUSE_PORT env var)
        user: ClickHouse user (defaults to CLICKHOUSE_USER env var)
        password: ClickHouse password (defaults to CLICKHOUSE_PASSWORD env var)
        
    Returns:
        Boolean indicating success
        
    Raises:
        Exception: If the insertion fails
    """
    if not data:
        logger.info("No data to insert")
        return True
    
    database = database or os.getenv("CLICKHOUSE_DB", "default")
    
    # Get table columns
    columns = get_table_columns(
        table_name=table_name,
        database=database,
        host=host,
        port=port,
        user=user,
        password=password
    )
    
    # Format column names
    column_names = ', '.join(f'"{col}"' for col in columns)
    
    # Format rows
    formatted_rows = []
    for row in data:
        formatted_values = []
        for col in columns:
            value = row.get(col)
            # Check if it's a UUID field (simple heuristic)
            is_uuid = col.lower().endswith('id') and col.lower() != 'id'
            formatted_values.append(format_value(value, is_uuid=is_uuid))
        
        formatted_rows.append(f"({', '.join(formatted_values)})")
    
    # Construct query
    query = f"""
        INSERT INTO {database}.{table_name} ({column_names})
        VALUES {', '.join(formatted_rows)}
    """
    
    # Execute query
    return execute_clickhouse_query(
        query=query,
        with_response=False,
        host=host,
        port=port,
        user=user,
        password=password
    ) 