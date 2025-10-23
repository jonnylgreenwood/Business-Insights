import duckdb
import pandas as pd
from pathlib import Path
import re

# --- CONFIG ---
dbs = {
    "L0_business_insights": "SQL/L0_business_insights.duckdb",
    "L1_business_insights": "SQL/L1_business_insights.duckdb",
    "L2_business_insights": "SQL/L2_business_insights.duckdb",
}

wiki_path = Path("/Users/jonnygreenwood/Forecast-Accuracy-Analysis.wiki")
wiki_path.mkdir(parents=True, exist_ok=True)

# Map Pandas dtypes to SQL-style types + optional length
PANDAS_TO_SQL = {
    "object": ("VARCHAR", 255),
    "string": ("VARCHAR", 255),
    "int8": ("TINYINT", 1),
    "int16": ("SMALLINT", 2),
    "int32": ("INTEGER", 4),
    "int64": ("BIGINT", 8),
    "float16": ("REAL", 4),
    "float32": ("FLOAT", 4),
    "float64": ("DOUBLE", 8),
    "bool": ("BOOLEAN", 1),
    "datetime64[ns]": ("DATE", None),
    "datetime64[ns, UTC]": ("DATE", None),
    "timedelta[ns]": ("INTERVAL", None),
    "category": ("ENUM", None),
}


def profile_table(con, table_name):
    """Return profiling summary — grouping sequential d_ columns."""
    # Fetch schema only
    cols_df = con.execute(f"DESCRIBE {table_name}").fetch_df()

    # Detect 'd_' sequential columns
    d_cols = sorted([c for c in cols_df['column_name'] if re.match(r"d_\d+", c)],
                    key=lambda x: int(x.split('_')[1]))
    non_d_cols = [c for c in cols_df['column_name'] if c not in d_cols]

    profiles = []

    # Summarize d_ columns as one group if many exist
    if len(d_cols) > 5:
        example = f"{d_cols[0]} → {d_cols[-1]}"
        profiles.append({
            "Column": example,
            "Type": "BIGINT",
            "PandasType": 'int64',
            "SQLType": 'BIGINT',
            "Length": '8',
            "Count": "varies",
            "Nulls": "varies",
            "Unique": "~",
            "Min": "0",
            "Max": "max per day",
            "Sample": f"{len(d_cols)} sequential daily columns"
        })
    else:
        non_d_cols += d_cols  # small tables, include normally

    # Profile the remaining columns properly
    for col in non_d_cols:
        s = con.execute(f"SELECT {col} FROM {table_name}").fetch_df()[col]
        sample = s.dropna().unique()[:3]
        pd_type = str(s.dtype)

        # --- map pandas dtype to SQL type ---
        sql_type, size = PANDAS_TO_SQL.get(pd_type, ("UNKNOWN", None))
        profiles.append({
            "Column": col,
            "PandasType": pd_type,
            "SQLType": sql_type,
            "Length": size if size else "",
            "Count": len(s),
            "Count": len(s),
            "Nulls": s.isna().sum(),
            "Unique": s.nunique(dropna=True),
            "Min": s.min() if pd.api.types.is_numeric_dtype(s) else None,
            "Max": s.max() if pd.api.types.is_numeric_dtype(s) else None,
            "Sample": str(sample.tolist()) if len(sample) > 0 else "[]"
        })
    return pd.DataFrame(profiles)


# --- MAIN LOOP ---
for db_name, db_path in dbs.items():
    print(f"📚 Profiling {db_name}")
    con = duckdb.connect(db_path, read_only=True)

    tables = con.execute("""
        SELECT table_name FROM duckdb_tables WHERE internal = FALSE
    """).fetchall()

    md = f"# 🗄️ {db_name}\n\nThis page contains table summaries and column profiles.\n\n"
    md += "## 📊 Database Tables\n\n| Table | Row Count |\n|--------|------------|\n"

    table_profiles = []

    for (table,) in tables:
        try:
            row_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            md += f"| `{table}` | {row_count:,} |\n"
            table_profiles.append((table, row_count))
        except Exception as e:
            print(f"⚠️ Could not count {table}: {e}")

    md += "\n---\n"

    for table, row_count in table_profiles:
        print(f"  🧩 Profiling {table}")
        try:
            prof = profile_table(con, table)
        except Exception as e:
            md += f"### `{table}` (⚠️ Error: {e})\n\n"
            continue

        md += f"## `{table}`\n\n"
        md += f"**Row count:** {row_count:,}\n\n"
        md += "| Column | SQL Type | Length | Count | Nulls | Unique | Min | Max | Sample |\n"
        md += "|---------|-----------|---------|--------|--------|---------|-----|-----|---------|\n"

        for _, r in prof.iterrows():
            sql_type = r.get("SQLType", r.get("Type", ""))
            length = r.get("Length", "")
            count = r.get("Count", "")
            nulls = r.get("Nulls", "")
            unique = r.get("Unique", "")
            min_val = r.get("Min", "")
            max_val = r.get("Max", "")
            sample = r.get("Sample", "")

            # Format numeric values as commas for readability
            if isinstance(count, (int, float)):
                count = f"{int(count):,}"
            if isinstance(nulls, (int, float)):
                nulls = f"{int(nulls):,}"
            if isinstance(unique, (int, float)):
                unique = f"{int(unique):,}"

            md += f"| `{r['Column']}` | {sql_type} | {length} | {count} | {nulls} | {unique} | {min_val} | {max_val} | {sample} |\n"

        md += "\n---\n"

    out_path = wiki_path / f"{db_name}.md"
    with open(out_path, "w") as f:
        f.write(md)

    con.close()
    print(f"✅ Wrote {out_path}")

print("\n✨ Wiki pages updated successfully.")
