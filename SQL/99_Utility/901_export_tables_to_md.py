import duckdb
import os
from tabulate import tabulate

# Paths
DB_PATH = "SQL/L2_forecast_accuracy.duckdb"
EXPORT_DIR = "exports"
os.makedirs(EXPORT_DIR, exist_ok=True)

# Connect
con = duckdb.connect(DB_PATH)

# List of L2 tables to export
tables = [
    "L2_forecast_accuracy.event_window_lifts",
    "L2_forecast_accuracy.calendar_events",
    'data_quality_kpis',
    'dim_calendar',
    'dim_product',
    'dim_sell_prices',
    'dim_store',
    'event_window_lifts',
    'event_windows_pearson_correlations',
    'l2_sales',
    'l2_sales_long',
    'sales_profile',
    'calendar_events'
]

# Sample limit
limit = 10   # change to None for full export

for table in tables:
    query = f"SELECT * FROM {table}"
    if limit:
        query += f" LIMIT {limit}"

    df = con.execute(query).fetchdf()
    md = tabulate(df, headers='keys', tablefmt='github')

    base = table.replace('.', '_')
    md_path = os.path.join(EXPORT_DIR, f"{base}.md")

    with open(md_path, "w") as f:
        f.write(md)

    print(f"✅ Exported {len(df):,} rows from {table} → {md_path}")

con.close()
