"""
Data Ingestion Module
Handles loading data from various sources including CSV, JSON, and databases.
"""

import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any
from sqlalchemy import create_engine


class DataIngestion:
    """Handles data ingestion from multiple sources."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the DataIngestion class.

        Args:
            config: Optional configuration dictionary
        """
        self.config = config or {}

    def load_csv(
        self,
        file_path: str,
        encoding: str = "utf-8",
        **kwargs
    ) -> pd.DataFrame:
        """
        Load data from a CSV file.

        Args:
            file_path: Path to the CSV file
            encoding: File encoding (default: utf-8)
            **kwargs: Additional arguments passed to pd.read_csv

        Returns:
            DataFrame containing the loaded data
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")

        df = pd.read_csv(path, encoding=encoding, **kwargs)
        print(f"Loaded {len(df)} rows from {file_path}")
        return df

    def load_json(
        self,
        file_path: str,
        **kwargs
    ) -> pd.DataFrame:
        """
        Load data from a JSON file.

        Args:
            file_path: Path to the JSON file
            **kwargs: Additional arguments passed to pd.read_json

        Returns:
            DataFrame containing the loaded data
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"JSON file not found: {file_path}")

        df = pd.read_json(path, **kwargs)
        print(f"Loaded {len(df)} rows from {file_path}")
        return df

    def load_from_database(
        self,
        query: str,
        connection_string: str
    ) -> pd.DataFrame:
        """
        Load data from a database using SQL query.

        Args:
            query: SQL query to execute
            connection_string: Database connection string

        Returns:
            DataFrame containing the query results
        """
        engine = create_engine(connection_string)
        df = pd.read_sql(query, engine)
        print(f"Loaded {len(df)} rows from database")
        return df

    def load_excel(
        self,
        file_path: str,
        sheet_name: Optional[str] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Load data from an Excel file.

        Args:
            file_path: Path to the Excel file
            sheet_name: Name of the sheet to load (default: first sheet)
            **kwargs: Additional arguments passed to pd.read_excel

        Returns:
            DataFrame containing the loaded data
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Excel file not found: {file_path}")

        df = pd.read_excel(path, sheet_name=sheet_name, **kwargs)
        print(f"Loaded {len(df)} rows from {file_path}")
        return df
