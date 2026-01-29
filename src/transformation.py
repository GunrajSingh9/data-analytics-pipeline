"""
Data Transformation Module
Handles data cleaning, validation, and transformation operations.
"""

import pandas as pd
import numpy as np
from typing import List, Optional, Dict, Any, Callable


class DataTransformer:
    """Handles data transformation and cleaning operations."""

    def __init__(self, df: Optional[pd.DataFrame] = None):
        """
        Initialize the DataTransformer.

        Args:
            df: Optional DataFrame to transform
        """
        self.df = df

    def set_dataframe(self, df: pd.DataFrame) -> "DataTransformer":
        """Set the DataFrame to transform."""
        self.df = df.copy()
        return self

    def remove_duplicates(
        self,
        subset: Optional[List[str]] = None,
        keep: str = "first"
    ) -> "DataTransformer":
        """
        Remove duplicate rows.

        Args:
            subset: Columns to consider for duplicates
            keep: Which duplicate to keep ('first', 'last', False)

        Returns:
            Self for method chaining
        """
        original_count = len(self.df)
        self.df = self.df.drop_duplicates(subset=subset, keep=keep)
        removed = original_count - len(self.df)
        print(f"Removed {removed} duplicate rows")
        return self

    def handle_missing_values(
        self,
        strategy: str = "drop",
        fill_value: Any = None,
        columns: Optional[List[str]] = None
    ) -> "DataTransformer":
        """
        Handle missing values in the DataFrame.

        Args:
            strategy: How to handle missing values ('drop', 'fill', 'ffill', 'bfill', 'mean', 'median')
            fill_value: Value to use when strategy is 'fill'
            columns: Specific columns to apply the strategy to

        Returns:
            Self for method chaining
        """
        cols = columns or self.df.columns.tolist()

        if strategy == "drop":
            self.df = self.df.dropna(subset=cols)
        elif strategy == "fill":
            self.df[cols] = self.df[cols].fillna(fill_value)
        elif strategy == "ffill":
            self.df[cols] = self.df[cols].ffill()
        elif strategy == "bfill":
            self.df[cols] = self.df[cols].bfill()
        elif strategy == "mean":
            for col in cols:
                if self.df[col].dtype in [np.float64, np.int64]:
                    self.df[col] = self.df[col].fillna(self.df[col].mean())
        elif strategy == "median":
            for col in cols:
                if self.df[col].dtype in [np.float64, np.int64]:
                    self.df[col] = self.df[col].fillna(self.df[col].median())

        print(f"Applied '{strategy}' strategy for missing values")
        return self

    def convert_types(
        self,
        type_mapping: Dict[str, str]
    ) -> "DataTransformer":
        """
        Convert column data types.

        Args:
            type_mapping: Dictionary mapping column names to target types

        Returns:
            Self for method chaining
        """
        for col, dtype in type_mapping.items():
            if col in self.df.columns:
                if dtype == "datetime":
                    self.df[col] = pd.to_datetime(self.df[col])
                else:
                    self.df[col] = self.df[col].astype(dtype)
        print(f"Converted types for {len(type_mapping)} columns")
        return self

    def add_calculated_column(
        self,
        column_name: str,
        calculation: Callable[[pd.DataFrame], pd.Series]
    ) -> "DataTransformer":
        """
        Add a new calculated column.

        Args:
            column_name: Name for the new column
            calculation: Function that takes DataFrame and returns Series

        Returns:
            Self for method chaining
        """
        self.df[column_name] = calculation(self.df)
        print(f"Added calculated column: {column_name}")
        return self

    def filter_rows(
        self,
        condition: Callable[[pd.DataFrame], pd.Series]
    ) -> "DataTransformer":
        """
        Filter rows based on a condition.

        Args:
            condition: Function that returns boolean Series

        Returns:
            Self for method chaining
        """
        original_count = len(self.df)
        self.df = self.df[condition(self.df)]
        filtered = original_count - len(self.df)
        print(f"Filtered out {filtered} rows")
        return self

    def rename_columns(
        self,
        mapping: Dict[str, str]
    ) -> "DataTransformer":
        """
        Rename columns.

        Args:
            mapping: Dictionary mapping old names to new names

        Returns:
            Self for method chaining
        """
        self.df = self.df.rename(columns=mapping)
        print(f"Renamed {len(mapping)} columns")
        return self

    def aggregate(
        self,
        group_by: List[str],
        aggregations: Dict[str, str]
    ) -> "DataTransformer":
        """
        Aggregate data by groups.

        Args:
            group_by: Columns to group by
            aggregations: Dictionary mapping columns to aggregation functions

        Returns:
            Self for method chaining
        """
        self.df = self.df.groupby(group_by).agg(aggregations).reset_index()
        print(f"Aggregated data by {group_by}")
        return self

    def get_dataframe(self) -> pd.DataFrame:
        """Return the transformed DataFrame."""
        return self.df.copy()

    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of the current DataFrame."""
        return {
            "rows": len(self.df),
            "columns": len(self.df.columns),
            "column_names": self.df.columns.tolist(),
            "dtypes": self.df.dtypes.to_dict(),
            "missing_values": self.df.isnull().sum().to_dict()
        }
