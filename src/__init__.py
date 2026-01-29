"""
Data Analytics Pipeline
A comprehensive data engineering solution for ETL, analytics, and reporting.
"""

from .ingestion import DataIngestion
from .transformation import DataTransformer
from .reporting import ReportGenerator
from .pipeline import DataPipeline

__version__ = "1.0.0"
__all__ = ["DataIngestion", "DataTransformer", "ReportGenerator", "DataPipeline"]
