"""
Data Analytics Pipeline Demo
Author: Gurpreet Singh (gsingh)

This script demonstrates the full ETL pipeline capabilities:
- Data ingestion from CSV
- Data transformation and cleaning
- Analytics report generation with visualizations
"""

from src import DataPipeline, DataTransformer, ReportGenerator
import pandas as pd


def main():
    print("=" * 60)
    print("  DATA ANALYTICS PIPELINE DEMO")
    print("  Author: Gurpreet Singh")
    print("=" * 60)
    
    # Initialize pipeline with configuration
    pipeline = DataPipeline("config/config.yaml")
    
    # Step 1: Extract data
    print("\n[1/4] Extracting data from CSV...")
    pipeline.extract("csv", "data/sample_sales.csv")
    
    # Step 2: Transform data
    print("\n[2/4] Transforming data...")
    pipeline.transform(
        remove_duplicates=True,
        handle_missing="drop",
        calculated_columns={
            "total_amount": lambda df: df["quantity"] * df["unit_price"]
        }
    )
    
    # Step 3: Generate reports
    print("\n[3/4] Generating analytics reports...")
    pipeline.generate_report(
        report_title="Sales Analytics Dashboard - by gsingh",
        charts=[
            {"type": "bar", "x": "category", "y": "total_amount", "title": "Revenue by Category"},
            {"type": "bar", "x": "region", "y": "quantity", "title": "Sales Volume by Region"},
            {"type": "heatmap", "title": "Correlation Analysis"}
        ]
    )
    
    # Step 4: Show summary
    print("\n[4/4] Pipeline Summary:")
    summary = pipeline.get_summary()
    print(f"   - Total rows processed: {summary['rows']}")
    print(f"   - Columns: {', '.join(summary['column_names'])}")
    print(f"   - Memory usage: {summary['memory_usage']}")
    
    # Additional analytics
    df = pipeline.get_data()
    
    print("\n" + "=" * 60)
    print("  KEY INSIGHTS")
    print("=" * 60)
    
    # Revenue by category
    print("\nRevenue by Category:")
    revenue_by_cat = df.groupby("category")["total_amount"].sum().sort_values(ascending=False)
    for cat, rev in revenue_by_cat.items():
        print(f"   {cat}: ${rev:,.2f}")
    
    # Top products
    print("\nTop 5 Products by Revenue:")
    top_products = df.groupby("product_name")["total_amount"].sum().sort_values(ascending=False).head()
    for i, (product, rev) in enumerate(top_products.items(), 1):
        print(f"   {i}. {product}: ${rev:,.2f}")
    
    # Regional performance
    print("\nSales by Region:")
    region_sales = df.groupby("region")["total_amount"].sum().sort_values(ascending=False)
    for region, rev in region_sales.items():
        print(f"   {region}: ${rev:,.2f}")
    
    print("\n" + "=" * 60)
    print("  Reports saved to: reports/")
    print("=" * 60)
    
    return df


if __name__ == "__main__":
    result = main()
