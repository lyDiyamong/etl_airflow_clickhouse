from abc import ABC, abstractmethod

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