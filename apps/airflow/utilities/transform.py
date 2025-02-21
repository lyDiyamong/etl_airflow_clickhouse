import logging
import pandas as pd
from .base import DataTransformer

logger = logging.getLogger(__name__)

class BaseTransformer(DataTransformer):
    """Base transformer with common transformation methods"""
    def transform(self, data):
        if not data:
            return []
        
        df = pd.DataFrame(data)
        df = self._apply_transformations(df)
        return df.to_dict('records')
    
    def _apply_transformations(self, df):
        """Apply transformations to the dataframe - override in subclasses"""
        return df


class TemplateTransformer(BaseTransformer):
    """Transformer for template data"""
    def _apply_transformations(self, df):
        # Add specific transformations for template data
        return df


class CustomTransformer(BaseTransformer):
    """Example of a custom transformer with specific logic"""
    def __init__(self, transformations=None):
        """
        Initialize with optional custom transformations
        
        Args:
            transformations: Dict of {column_name: transformation_function}
        """
        self.transformations = transformations or {}
    
    def _apply_transformations(self, df):
        # Apply any registered custom transformations
        for column, transform_fn in self.transformations.items():
            if column in df.columns:
                try:
                    df[column] = df[column].apply(transform_fn)
                except Exception as e:
                    logger.warning(f"Failed to transform column {column}: {str(e)}")
        
        return df