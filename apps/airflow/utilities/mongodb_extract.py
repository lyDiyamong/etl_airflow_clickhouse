import logging
import pandas as pd
from pymongo import MongoClient
from .base import DataSource

logger = logging.getLogger(__name__)

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
            logger.error(f"MongoDB extraction error: {str(e)}")
            raise
            
    def close(self):
        if self.client:
            self.client.close()