# Data Analytics Pipeline

**Author:** Gurpreet Singh (gsingh)  
**Built with:** Python 3.12 | pandas | matplotlib | seaborn

A comprehensive Python-based ETL (Extract, Transform, Load) pipeline for data analytics and reporting. This project demonstrates data engineering best practices including modular architecture, data validation, and automated report generation.

## Features

- **Data Ingestion**: Load data from CSV, JSON, Excel, and databases
- **Data Transformation**: Clean, validate, and transform data with method chaining
- **Reporting**: Generate HTML reports and visualizations (bar charts, line charts, heatmaps)
- **Pipeline Orchestration**: Run complete ETL workflows with configuration files

## Project Structure

```
data-analytics-pipeline/
├── src/
│   ├── __init__.py
│   ├── ingestion.py      # Data loading from various sources
│   ├── transformation.py # Data cleaning and transformation
│   ├── reporting.py      # Report and visualization generation
│   └── pipeline.py       # Main pipeline orchestrator
├── data/
│   └── sample_sales.csv  # Sample dataset
├── config/
│   └── config.yaml       # Pipeline configuration
├── reports/              # Generated reports output
├── tests/
│   └── test_pipeline.py  # Unit tests
├── requirements.txt
└── README.md
```

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd data-analytics-pipeline
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Quick Start

### Basic Usage

```python
from src import DataPipeline

# Initialize pipeline
pipeline = DataPipeline()

# Run ETL workflow
pipeline.extract("csv", "data/sample_sales.csv")
pipeline.transform(remove_duplicates=True, handle_missing="drop")
pipeline.generate_report("Sales Analytics Report")

# Get processed data
df = pipeline.get_data()
print(df.head())
```

### Using Individual Components

```python
from src import DataIngestion, DataTransformer, ReportGenerator

# Load data
ingestion = DataIngestion()
df = ingestion.load_csv("data/sample_sales.csv")

# Transform data
transformer = DataTransformer(df)
cleaned_df = (
    transformer
    .remove_duplicates()
    .handle_missing_values(strategy="mean")
    .add_calculated_column("total", lambda x: x["quantity"] * x["unit_price"])
    .get_dataframe()
)

# Generate reports
reporter = ReportGenerator("reports")
reporter.generate_html_report(cleaned_df, "Sales Report")
reporter.create_bar_chart(cleaned_df, "category", "total", "Sales by Category")
```

### Full Pipeline with Configuration

```python
from src import DataPipeline

pipeline = DataPipeline("config/config.yaml")

result = pipeline.run_full_pipeline(
    source_config={
        "type": "csv",
        "path": "data/sample_sales.csv"
    },
    transform_config={
        "remove_duplicates": True,
        "handle_missing": "drop"
    },
    report_config={
        "report_title": "Sales Analytics Dashboard",
        "charts": [
            {"type": "bar", "x": "category", "y": "quantity", "title": "Quantity by Category"},
            {"type": "heatmap", "title": "Correlation Matrix"}
        ]
    }
)
```

## Running Tests

```bash
pytest tests/ -v
```

## Key Components

### DataIngestion
- `load_csv()` - Load CSV files
- `load_json()` - Load JSON files
- `load_excel()` - Load Excel files
- `load_from_database()` - Execute SQL queries

### DataTransformer
- `remove_duplicates()` - Remove duplicate rows
- `handle_missing_values()` - Handle NaN values (drop, fill, mean, median)
- `convert_types()` - Convert column data types
- `add_calculated_column()` - Add computed columns
- `filter_rows()` - Filter data by conditions
- `aggregate()` - Group and aggregate data

### ReportGenerator
- `generate_summary_statistics()` - Statistical summaries
- `create_bar_chart()` - Bar chart visualizations
- `create_line_chart()` - Line chart visualizations
- `create_pie_chart()` - Pie chart visualizations
- `create_heatmap()` - Correlation heatmaps
- `generate_html_report()` - Full HTML reports
- `export_to_csv()` - Export processed data

## Technologies Used

- **Python 3.8+**
- **pandas** - Data manipulation
- **numpy** - Numerical computing
- **matplotlib** - Plotting
- **seaborn** - Statistical visualizations
- **SQLAlchemy** - Database connectivity
- **pytest** - Testing framework

## License

MIT License
