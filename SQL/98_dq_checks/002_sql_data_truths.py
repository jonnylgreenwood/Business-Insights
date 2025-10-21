import duckdb
import pandas as pd
import numpy as np
from pathlib import Path

# ----------------------------------------------------------------------------- #
# CONFIGURATION
# ----------------------------------------------------------------------------- #

SQL_DIR = Path("SQL")
L0_DB = SQL_DIR / "L0_forecast_accuracy.duckdb"
L1_DB = SQL_DIR / "L1_forecast_accuracy.duckdb"
L2_DB = SQL_DIR / "L2_forecast_accuracy.duckdb"

OUTPUT_TABLE = "dq_truths_sql"

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


def list_tables(db_path: Path) -> list:
    """List all tables in a DuckDB database."""
    con = duckdb.connect(str(db_path))
    try:
        tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    except Exception as e:
        print(f"⚠️ Could not list tables in {db_path.name}: {e}")
        tables = []
    con.close()
    return tables


def profile_table(db_path: Path, table_name: str):
    """Profile a DuckDB table and return a DataFrame of metrics."""
    print(f"🔍 Profiling {db_path.stem}.{table_name} ...")
    con = duckdb.connect(str(db_path))
    try:
        df = con.execute(f"SELECT * FROM {table_name}").fetchdf()
    except Exception as e:
        print(f"⚠️ Could not read {table_name}: {e}")
        return pd.DataFrame()
    finally:
        con.close()

    results = []
    source = db_path.stem
    row_count = len(df)
    vi, vf, vb, vd, vs = split_value_types(row_count)
    results.append((source, table_name, None, "row_count", "INTEGER", vi, vf, vb, vd, vs))

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

        # Add metrics
        for metric_type, value in metrics.items():
            vi, vf, vb, vd, vs = split_value_types(value)
            results.append((source, table_name, col, metric_type, dtype, vi, vf, vb, vd, vs))

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

    for db_path in [L0_DB, L1_DB, L2_DB]:
        print(f"\n📂 Connecting to {db_path.name}")
        for table in list_tables(db_path):
            df_metrics = profile_table(db_path, table)
            if not df_metrics.empty:
                all_results.append(df_metrics)

    if not all_results:
        print("⚠️ No tables found across L0–L2.")
        return

    dq_truths_df = pd.concat(all_results, ignore_index=True)
    print(f"\n📊 Generated {len(dq_truths_df)} total metric rows.")

    # Store in L2 DB (or skip if just testing)
    con = duckdb.connect(str(L2_DB))
    con.execute(f"CREATE OR REPLACE TABLE {OUTPUT_TABLE} AS SELECT * FROM dq_truths_df")
    con.close()

    print(f"💾 Stored SQL profiling results in {L2_DB}:{OUTPUT_TABLE}")
    print("✅ Done.")


if __name__ == "__main__":
    main()
