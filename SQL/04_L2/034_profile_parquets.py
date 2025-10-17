import duckdb
import pandas as pd
import glob
from pathlib import Path

# Connect (no file = in-memory DuckDB)
con = duckdb.connect()

profiles = []

for path in glob.glob('SQL/outputs/parquet/*.parquet'):
    table_name = Path(path).stem
    print(f"Profiling {table_name} …")

    df = con.execute(f"SELECT * FROM parquet_scan('{path}')").df()

    for col in df.columns:
        s = df[col]
        profiles.append({
            "table_name": table_name,
            "column_name": col,
            "dtype": str(s.dtype),
            "rows": len(s),
            "unique_values": s.nunique(dropna=True),
            "nulls": s.isna().sum(),
        })

# Save as CSV
csv_path = "SQL/outputs/csv_for_markdown/data_profile.csv"
md_path  = "SQL/outputs/csv_for_markdown_complete/data_profile.md"
Path(csv_path).parent.mkdir(parents=True, exist_ok=True)

df_profile = pd.DataFrame(profiles)
df_profile.to_csv(csv_path, index=False)

# Convert to Markdown
with open(md_path, "w") as f:
    f.write(df_profile.to_markdown(index=False))

print(f"✅ Saved:\n- {csv_path}\n- {md_path}")
