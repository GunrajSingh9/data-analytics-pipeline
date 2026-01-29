"""
Data Pipeline Module
Orchestrates the complete ETL pipeline from ingestion to reporting.
"""

import yaml
from pathlib import Path
from typing import Optional, Dict, Any
import pandas as pd

from .ingestion import DataIngestion
from .transformation import DataTransformer
from .reporting import ReportGenerator


class DataPipeline:
    """Orchestrates the complete data analytics pipeline."""

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the DataPipeline.

        Args:
            config_path: Path to YAML configuration file
        """
        self.config = self._load_config(config_path) if config_path else {}
        self.ingestion = DataIngestion(self.config.get("ingestion", {}))
        self.transformer = DataTransformer()
        self.reporter = ReportGenerator(
            self.config.get("reporting", {}).get("output_dir", "reports")
        )
        self.data: Optional[pd.DataFrame] = None

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(path, "r") as f:
            return yaml.safe_load(f)

    def extract(
        self,
        source_type: str,
        source_path: str,
        **kwargs
    ) -> "DataPipeline":
        """
        Extract data from a source.

        Args:
            source_type: Type of source ('csv', 'json', 'excel', 'database')
            source_path: Path to source or connection string
            **kwargs: Additional arguments for the loader

        Returns:
            Self for method chaining
        """
        if source_type == "csv":
            self.data = self.ingestion.load_csv(source_path, **kwargs)
        elif source_type == "json":
            self.data = self.ingestion.load_json(source_path, **kwargs)
        elif source_type == "excel":
            self.data = self.ingestion.load_excel(source_path, **kwargs)
        elif source_type == "database":
            query = kwargs.pop("query")
            self.data = self.ingestion.load_from_database(query, source_path)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")

        self.transformer.set_dataframe(self.data)
        return self

    def transform(
        self,
        remove_duplicates: bool = True,
        handle_missing: Optional[str] = None,
        type_conversions: Optional[Dict[str, str]] = None,
        calculated_columns: Optional[Dict[str, Any]] = None
    ) -> "DataPipeline":
        """
        Apply transformations to the data.

        Args:
            remove_duplicates: Whether to remove duplicate rows
            handle_missing: Strategy for handling missing values
            type_conversions: Dictionary of column type conversions
            calculated_columns: Dictionary of calculated column definitions

        Returns:
            Self for method chaining
        """
        if self.data is None:
            raise ValueError("No data loaded. Run extract() first.")

        if remove_duplicates:
            self.transformer.remove_duplicates()

        if handle_missing:
            self.transformer.handle_missing_values(strategy=handle_missing)

        if type_conversions:
            self.transformer.convert_types(type_conversions)

        if calculated_columns:
            for name, calc in calculated_columns.items():
                self.transformer.add_calculated_column(name, calc)

        self.data = self.transformer.get_dataframe()
        return self

    def load(
        self,
        destination_type: str,
        destination_path: str,
        **kwargs
    ) -> "DataPipeline":
        """
        Load data to a destination.

        Args:
            destination_type: Type of destination ('csv', 'excel', 'database')
            destination_path: Path to destination or connection string
            **kwargs: Additional arguments

        Returns:
            Self for method chaining
        """
        if self.data is None:
            raise ValueError("No data to load. Run extract() first.")

        if destination_type == "csv":
            self.data.to_csv(destination_path, index=False, **kwargs)
            print(f"Data saved to CSV: {destination_path}")
        elif destination_type == "excel":
            self.data.to_excel(destination_path, index=False, **kwargs)
            print(f"Data saved to Excel: {destination_path}")
        else:
            raise ValueError(f"Unsupported destination type: {destination_type}")

        return self

    def generate_report(
        self,
        report_title: str = "Data Analytics Report",
        charts: Optional[list] = None
    ) -> "DataPipeline":
        """
        Generate analytics report with visualizations.

        Args:
            report_title: Title for the report
            charts: List of chart configurations

        Returns:
            Self for method chaining
        """
        if self.data is None:
            raise ValueError("No data loaded. Run extract() first.")

        # Generate HTML report
        self.reporter.generate_html_report(self.data, report_title)

        # Generate charts if specified
        if charts:
            for chart_config in charts:
                chart_type = chart_config.get("type")
                if chart_type == "bar":
                    self.reporter.create_bar_chart(
                        self.data,
                        chart_config["x"],
                        chart_config["y"],
                        chart_config.get("title", "Bar Chart")
                    )
                elif chart_type == "line":
                    self.reporter.create_line_chart(
                        self.data,
                        chart_config["x"],
                        chart_config["y"],
                        chart_config.get("title", "Line Chart"),
                        chart_config.get("hue")
                    )
                elif chart_type == "heatmap":
                    self.reporter.create_heatmap(
                        self.data,
                        chart_config.get("title", "Correlation Heatmap")
                    )

        return self

    def get_data(self) -> pd.DataFrame:
        """Return the current DataFrame."""
        if self.data is None:
            raise ValueError("No data loaded.")
        return self.data.copy()

    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of the current data."""
        if self.data is None:
            return {"status": "No data loaded"}

        return {
            "rows": len(self.data),
            "columns": len(self.data.columns),
            "column_names": self.data.columns.tolist(),
            "memory_usage": f"{self.data.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB"
        }

    def run_full_pipeline(
        self,
        source_config: Dict[str, Any],
        transform_config: Optional[Dict[str, Any]] = None,
        report_config: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """
        Run the complete ETL pipeline.

        Args:
            source_config: Configuration for data extraction
            transform_config: Configuration for transformations
            report_config: Configuration for reporting

        Returns:
            The processed DataFrame
        """
        # Extract
        self.extract(
            source_config["type"],
            source_config["path"],
            **source_config.get("options", {})
        )

        # Transform
        if transform_config:
            self.transform(**transform_config)

        # Report
        if report_config:
            self.generate_report(**report_config)

        print("\nPipeline completed successfully!")
        print(f"Processed {len(self.data)} rows")
        return self.data
