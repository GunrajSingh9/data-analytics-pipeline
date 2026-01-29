"""
Reporting Module
Generates analytics reports and visualizations.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime


class ReportGenerator:
    """Generates analytics reports and visualizations."""

    def __init__(self, output_dir: str = "reports"):
        """
        Initialize the ReportGenerator.

        Args:
            output_dir: Directory to save reports
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.figures: List[plt.Figure] = []

        # Set default style
        sns.set_theme(style="whitegrid")
        plt.rcParams["figure.figsize"] = (10, 6)

    def generate_summary_statistics(
        self,
        df: pd.DataFrame,
        numeric_only: bool = True
    ) -> pd.DataFrame:
        """
        Generate summary statistics for the DataFrame.

        Args:
            df: Input DataFrame
            numeric_only: Whether to include only numeric columns

        Returns:
            DataFrame with summary statistics
        """
        if numeric_only:
            summary = df.describe()
        else:
            summary = df.describe(include="all")

        return summary

    def create_bar_chart(
        self,
        df: pd.DataFrame,
        x_column: str,
        y_column: str,
        title: str,
        save: bool = True
    ) -> plt.Figure:
        """
        Create a bar chart.

        Args:
            df: Input DataFrame
            x_column: Column for x-axis
            y_column: Column for y-axis
            title: Chart title
            save: Whether to save the chart

        Returns:
            Matplotlib Figure
        """
        fig, ax = plt.subplots()
        sns.barplot(data=df, x=x_column, y=y_column, ax=ax)
        ax.set_title(title)
        ax.set_xlabel(x_column)
        ax.set_ylabel(y_column)
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()

        if save:
            self._save_figure(fig, f"bar_chart_{x_column}_{y_column}")

        self.figures.append(fig)
        return fig

    def create_line_chart(
        self,
        df: pd.DataFrame,
        x_column: str,
        y_column: str,
        title: str,
        hue: Optional[str] = None,
        save: bool = True
    ) -> plt.Figure:
        """
        Create a line chart.

        Args:
            df: Input DataFrame
            x_column: Column for x-axis
            y_column: Column for y-axis
            title: Chart title
            hue: Column for color grouping
            save: Whether to save the chart

        Returns:
            Matplotlib Figure
        """
        fig, ax = plt.subplots()
        sns.lineplot(data=df, x=x_column, y=y_column, hue=hue, ax=ax)
        ax.set_title(title)
        ax.set_xlabel(x_column)
        ax.set_ylabel(y_column)
        plt.tight_layout()

        if save:
            self._save_figure(fig, f"line_chart_{x_column}_{y_column}")

        self.figures.append(fig)
        return fig

    def create_pie_chart(
        self,
        df: pd.DataFrame,
        values_column: str,
        labels_column: str,
        title: str,
        save: bool = True
    ) -> plt.Figure:
        """
        Create a pie chart.

        Args:
            df: Input DataFrame
            values_column: Column with values
            labels_column: Column with labels
            title: Chart title
            save: Whether to save the chart

        Returns:
            Matplotlib Figure
        """
        fig, ax = plt.subplots()
        ax.pie(
            df[values_column],
            labels=df[labels_column],
            autopct="%1.1f%%",
            startangle=90
        )
        ax.set_title(title)
        plt.tight_layout()

        if save:
            self._save_figure(fig, f"pie_chart_{values_column}")

        self.figures.append(fig)
        return fig

    def create_heatmap(
        self,
        df: pd.DataFrame,
        title: str,
        save: bool = True
    ) -> plt.Figure:
        """
        Create a correlation heatmap.

        Args:
            df: Input DataFrame (numeric columns only)
            title: Chart title
            save: Whether to save the chart

        Returns:
            Matplotlib Figure
        """
        numeric_df = df.select_dtypes(include=["number"])
        correlation = numeric_df.corr()

        fig, ax = plt.subplots(figsize=(12, 8))
        sns.heatmap(
            correlation,
            annot=True,
            cmap="coolwarm",
            center=0,
            ax=ax
        )
        ax.set_title(title)
        plt.tight_layout()

        if save:
            self._save_figure(fig, "correlation_heatmap")

        self.figures.append(fig)
        return fig

    def generate_html_report(
        self,
        df: pd.DataFrame,
        report_title: str,
        include_summary: bool = True
    ) -> str:
        """
        Generate an HTML report.

        Args:
            df: Input DataFrame
            report_title: Title for the report
            include_summary: Whether to include summary statistics

        Returns:
            Path to the generated HTML file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"report_{timestamp}.html"
        filepath = self.output_dir / filename

        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>{report_title}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; }}
        h1 {{ color: #333; }}
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #4CAF50; color: white; }}
        tr:nth-child(even) {{ background-color: #f2f2f2; }}
        .summary {{ background-color: #f9f9f9; padding: 20px; margin: 20px 0; }}
    </style>
</head>
<body>
    <h1>{report_title}</h1>
    <p>Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
"""

        if include_summary:
            summary = self.generate_summary_statistics(df)
            html_content += f"""
    <div class="summary">
        <h2>Summary Statistics</h2>
        {summary.to_html()}
    </div>
"""

        html_content += f"""
    <h2>Data Preview (First 100 rows)</h2>
    {df.head(100).to_html(index=False)}
</body>
</html>
"""

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(html_content)

        print(f"HTML report generated: {filepath}")
        return str(filepath)

    def export_to_csv(
        self,
        df: pd.DataFrame,
        filename: str
    ) -> str:
        """
        Export DataFrame to CSV.

        Args:
            df: Input DataFrame
            filename: Output filename

        Returns:
            Path to the exported file
        """
        filepath = self.output_dir / filename
        df.to_csv(filepath, index=False)
        print(f"Data exported to: {filepath}")
        return str(filepath)

    def _save_figure(self, fig: plt.Figure, name: str) -> str:
        """Save a figure to the output directory."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{name}_{timestamp}.png"
        filepath = self.output_dir / filename
        fig.savefig(filepath, dpi=300, bbox_inches="tight")
        print(f"Figure saved: {filepath}")
        return str(filepath)

    def close_all_figures(self):
        """Close all matplotlib figures."""
        for fig in self.figures:
            plt.close(fig)
        self.figures.clear()
