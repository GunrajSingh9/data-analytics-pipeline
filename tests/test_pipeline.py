"""
Unit tests for the Data Analytics Pipeline.
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import tempfile
import os

# Add src to path for imports
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ingestion import DataIngestion
from src.transformation import DataTransformer
from src.reporting import ReportGenerator


class TestDataIngestion:
    """Tests for DataIngestion class."""

    def test_load_csv(self, tmp_path):
        """Test CSV loading."""
        # Create test CSV
        csv_path = tmp_path / "test.csv"
        test_df = pd.DataFrame({
            "col1": [1, 2, 3],
            "col2": ["a", "b", "c"]
        })
        test_df.to_csv(csv_path, index=False)

        # Load and verify
        ingestion = DataIngestion()
        result = ingestion.load_csv(str(csv_path))
        
        assert len(result) == 3
        assert list(result.columns) == ["col1", "col2"]

    def test_load_csv_file_not_found(self):
        """Test CSV loading with non-existent file."""
        ingestion = DataIngestion()
        
        with pytest.raises(FileNotFoundError):
            ingestion.load_csv("nonexistent.csv")

    def test_load_json(self, tmp_path):
        """Test JSON loading."""
        json_path = tmp_path / "test.json"
        test_df = pd.DataFrame({
            "col1": [1, 2, 3],
            "col2": ["a", "b", "c"]
        })
        test_df.to_json(json_path)

        ingestion = DataIngestion()
        result = ingestion.load_json(str(json_path))
        
        assert len(result) == 3


class TestDataTransformer:
    """Tests for DataTransformer class."""

    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame for testing."""
        return pd.DataFrame({
            "id": [1, 2, 2, 3, 4],
            "value": [10.0, 20.0, 20.0, np.nan, 40.0],
            "category": ["A", "B", "B", "C", "A"]
        })

    def test_remove_duplicates(self, sample_df):
        """Test duplicate removal."""
        transformer = DataTransformer(sample_df)
        result = transformer.remove_duplicates().get_dataframe()
        
        assert len(result) == 4  # One duplicate removed

    def test_handle_missing_drop(self, sample_df):
        """Test missing value handling with drop strategy."""
        transformer = DataTransformer(sample_df)
        result = transformer.handle_missing_values(strategy="drop").get_dataframe()
        
        assert result["value"].isna().sum() == 0

    def test_handle_missing_fill(self, sample_df):
        """Test missing value handling with fill strategy."""
        transformer = DataTransformer(sample_df)
        result = transformer.handle_missing_values(
            strategy="fill",
            fill_value=0,
            columns=["value"]
        ).get_dataframe()
        
        assert result["value"].isna().sum() == 0
        assert 0 in result["value"].values

    def test_convert_types(self, sample_df):
        """Test type conversion."""
        transformer = DataTransformer(sample_df)
        result = transformer.convert_types({"id": "str"}).get_dataframe()
        
        # pandas 3.0+ uses StringDtype, older versions use object
        assert pd.api.types.is_string_dtype(result["id"])

    def test_add_calculated_column(self, sample_df):
        """Test adding calculated column."""
        transformer = DataTransformer(sample_df)
        result = transformer.add_calculated_column(
            "value_doubled",
            lambda df: df["value"] * 2
        ).get_dataframe()
        
        assert "value_doubled" in result.columns

    def test_rename_columns(self, sample_df):
        """Test column renaming."""
        transformer = DataTransformer(sample_df)
        result = transformer.rename_columns({"id": "identifier"}).get_dataframe()
        
        assert "identifier" in result.columns
        assert "id" not in result.columns

    def test_method_chaining(self, sample_df):
        """Test method chaining."""
        transformer = DataTransformer(sample_df)
        result = (
            transformer
            .remove_duplicates()
            .handle_missing_values(strategy="fill", fill_value=0, columns=["value"])
            .get_dataframe()
        )
        
        assert len(result) == 4
        assert result["value"].isna().sum() == 0


class TestReportGenerator:
    """Tests for ReportGenerator class."""

    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame for testing."""
        return pd.DataFrame({
            "category": ["A", "B", "C", "A", "B"],
            "value": [100, 200, 150, 120, 180],
            "quantity": [5, 10, 7, 6, 8]
        })

    def test_generate_summary_statistics(self, sample_df):
        """Test summary statistics generation."""
        reporter = ReportGenerator()
        summary = reporter.generate_summary_statistics(sample_df)
        
        assert "mean" in summary.index
        assert "std" in summary.index
        reporter.close_all_figures()

    def test_generate_html_report(self, sample_df, tmp_path):
        """Test HTML report generation."""
        reporter = ReportGenerator(output_dir=str(tmp_path))
        filepath = reporter.generate_html_report(sample_df, "Test Report")
        
        assert Path(filepath).exists()
        assert filepath.endswith(".html")
        reporter.close_all_figures()

    def test_export_to_csv(self, sample_df, tmp_path):
        """Test CSV export."""
        reporter = ReportGenerator(output_dir=str(tmp_path))
        filepath = reporter.export_to_csv(sample_df, "export.csv")
        
        assert Path(filepath).exists()
        
        # Verify exported data
        exported = pd.read_csv(filepath)
        assert len(exported) == len(sample_df)
        reporter.close_all_figures()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
