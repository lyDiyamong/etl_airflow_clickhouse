from airflow.models import Variable
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def update_etl_timestamp(variable_key: str):
    """
    Creates a function that updates an Airflow Variable with the current timestamp.
    
    Args:
        variable_key: The name of the Airflow Variable to update
        
    Returns:
        A function that can be called to update the timestamp
    """
    def update_timestamp(**kwargs):
        """
        Updates the specified Airflow Variable with the current timestamp.
        
        Returns:
            The timestamp that was set
        """
        current_time = datetime.now().isoformat()
        Variable.set(variable_key, current_time)
        logger.info(f"Updated ETL timestamp for '{variable_key}' to: {current_time}")
        return current_time
        
    return update_timestamp