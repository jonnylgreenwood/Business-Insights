import pandas as pd

parquet_path = 'SQL/outputs/parquet/fact_sales_enriched.parquet'
csv_path = 'SQL/outputs/parquet/fact_sales_enriched.csv'

# Uses PyArrow engine under the hood
df = pd.read_parquet(parquet_path)
df.to_csv(csv_path, index=False)

print("✅ Exported:", csv_path)

