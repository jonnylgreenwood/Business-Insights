import os
import pandas as pd
import duckdb
from pathlib import Path
import numpy as np

# ----------------------------------------------------------------------------- #
# CONFIGURATION
# ----------------------------------------------------------------------------- #

RAW_DIR = Path("data/m5-forecasting-accuracy")  # folder where your raw CSVs live
L2_DB = Path("SQL/L2_forecast_accuracy.duckdb")
OUTPUT_TABLE = "dq_truths"

RAW_DIR.mkdir(parents=True, exist_ok=True)
L2_DB.parent.mkdir(parents=True, exist_ok=True)

# ----------------------------------------------------------------------------- #
# HELPERS
# ----------------------------------------------------------------------------- #

def map_dtype(dtype: str) -> str:
    """Convert Pandas dtype to SQL-style type."""
    dtype = str(dtype).lower()
    if "int" in dtype:
        return "INTEGER"
    elif "float" in dtype or "double" in dtype or "decimal" in dtype:
        return "DOUBLE"
    elif "bool" in dtype:
        return "BOOLEAN"
    elif "datetime" in dtype or "date" in dtype:
        return "DATETIME"
    elif "object" in dtype or "string" in dtype:
        return "VARCHAR"
    else:
        return "UNKNOWN"


def split_value_types(value):
    """Split a Python value into one of the typed value columns."""
    if pd.isna(value):
        return None, None, None, None, None

    if isinstance(value, bool):
        return None, None, value, None, None
    elif isinstance(value, (int, np.integer)):
        return value, None, None, None, None
    elif isinstance(value, (float, np.floating)):
        return None, value, None, None, None
    elif isinstance(value, (pd.Timestamp, np.datetime64)):
        return None, None, None, pd.to_datetime(value).date(), None
    else:
        return None, None, None, None, str(value)


def profile_csv(file_path: Path):
    """Profile one CSV file and return a DataFrame of metrics."""
    print(f"🔍 Profiling {file_path.name} ...")
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"⚠️ Could not read {file_path.name}: {e}")
        return pd.DataFrame()

    table_name = file_path.stem.lower()
    results = []

    # Table-level row count
    row_count = len(df)
    vi, vf, vb, vd, vs = split_value_types(row_count)
    results.append((file_path.name, table_name, None, "row_count", "INTEGER", vi, vf, vb, vd, vs))

    for col in df.columns:
        series = df[col]
        dtype = map_dtype(series.dtype)

        # Null count
        null_count = series.isna().sum()
        if dtype == "VARCHAR":
            null_count += (series.astype(str).str.strip() == "").sum()

        # Distinct count
        distinct_count = series.nunique(dropna=True)

        metrics = {
            "null_count": null_count,
            "distinct_count": distinct_count,
            "dtype": dtype,
        }

        # Numeric metrics
        if dtype in ["INTEGER", "DOUBLE"]:
            s = pd.to_numeric(series, errors="coerce")
            metrics.update({
                "sum": float(np.nansum(s)),
                "min": float(np.nanmin(s)) if not np.isnan(np.nanmin(s)) else None,
                "max": float(np.nanmax(s)) if not np.isnan(np.nanmax(s)) else None,
            })

        # Date metrics
        elif dtype == "DATETIME":
            s = pd.to_datetime(series, errors="coerce")
            if not s.dropna().empty:
                metrics.update({
                    "min": s.min().strftime("%Y-%m-%d"),
                    "max": s.max().strftime("%Y-%m-%d"),
                })

        # Add all metrics to results
        for metric_type, value in metrics.items():
            vi, vf, vb, vd, vs = split_value_types(value)
            results.append((file_path.name, table_name, col, metric_type, dtype, vi, vf, vb, vd, vs))

    return pd.DataFrame(
        results,
        columns=[
            "source",
            "table_name",
            "column_name",
            "metric_type",
            "dtype",
            "value_int",
            "value_float",
            "value_bool",
            "value_date",
            "value_str",
        ],
    )


# ----------------------------------------------------------------------------- #
# MAIN EXECUTION
# ----------------------------------------------------------------------------- #

def main():
    all_results = []

    for file_path in RAW_DIR.glob("*.csv"):
        df_metrics = profile_csv(file_path)
        if not df_metrics.empty:
            all_results.append(df_metrics)

    if not all_results:
        print(f"⚠️ No CSV files found in {RAW_DIR}")
        return

    dq_truths_df = pd.concat(all_results, ignore_index=True)
    print(f"\n📊 Generated {len(dq_truths_df)} total metric rows.")

    # Write to DuckDB
    con = duckdb.connect(str(L2_DB))
    con.execute(f"CREATE OR REPLACE TABLE {OUTPUT_TABLE} AS SELECT * FROM dq_truths_df")
    con.close()
    print(f"💾 Stored dq_truths in {L2_DB}:{OUTPUT_TABLE}")
    print("✅ Done.")


if __name__ == "__main__":
    main()
