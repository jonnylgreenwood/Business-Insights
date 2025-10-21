import duckdb
import pandas as pd
import numpy as np
from pathlib import Path

# ----------------------------------------------------------------------------- #
# CONFIGURATION
# ----------------------------------------------------------------------------- #

SQL_DIR = Path("SQL")
L2_DB = SQL_DIR / "L2_forecast_accuracy.duckdb"
OUTPUT_TABLE = "dq_results"
TOLERANCE_PCT = 0.05  # ±5%

# Mapping between "truth" data and target tables per layer
TABLE_LINKS = [
    {"truth": "calendar.csv",               "L0": "calendar",                 "L1": "dim_calendar",   "L2": "dim_calendar"},
    {"truth": "sales_train_evaluation.csv", "L0": "sales_train_evaluation",   "L1": "l1_sales",       "L2": "l2_sales"},
    {"truth": "sales_train_validation.csv", "L0": "sales_train_validation",   "L1": None,             "L2": "l2_sales_long"},
    {"truth": "sample_submission.csv",      "L0": "sample_submission",        "L1": None,             "L2": None},
    {"truth": "sell_prices.csv",            "L0": "sell_prices",              "L1": "dim_sell_prices","L2": "dim_sell_prices"},
]

# ----------------------------------------------------------------------------- #
# TABLE + LAYER-LEVEL EXCLUSIONS
# ----------------------------------------------------------------------------- #

# ----------------------------------------------------------------------------- #
# LAYER + TABLE-LEVEL EXCLUSIONS
# ----------------------------------------------------------------------------- #

EXCLUDED_COLUMNS = {
    "L0": {

    },
    "L1": {
        "l1_sales": ['column_name','cat_id','dept_id','date_key','id','item_id','product_key','sales_key','sell_price_key','state_id','store_id','store_key'],
        "dim_calendar": ['date_key'],
        "dim_sell_prices": ['column_name', 'store_key', 'store_id', 'sell_price_key', 'product_key', 'item_id'],
        'l1_sales': ['column_name', 'store_key', 'store_id', 'state_id', 'product_key', 'sales_key', 'item_id', 'id', 'dept_id', 'cat_id']
    },
    "L2": {
        "dim_sell_prices": ['column_name', 'store_key', 'store_id', 'sell_price_key', 'product_key', 'item_id'],
        'l2_sales': ['column_name', 'store_key', 'store_id', 'state_id', 'product_key', 'sales_key', 'item_id', 'id', 'dept_id', 'cat_id']
    },
}



NUMERIC_METRICS = {"row_count", "null_count", "distinct_count", "sum", "min", "max"}
TABLE_LEVEL_METRICS = {"row_count"}

# ----------------------------------------------------------------------------- #
# LOAD
# ----------------------------------------------------------------------------- #

def flatten_values(df, out_col="value_flat"):
    """Combine typed value columns into a single comparable field."""
    df[out_col] = (
        df["value_int"]
        .combine_first(df["value_float"])
        .combine_first(df["value_date"].astype(str))
        .combine_first(df["value_str"])
        .combine_first(df["value_bool"].astype(str))
    )
    return df


print("🔍 Loading dq_truths and dq_truths_sql from DuckDB...")
con = duckdb.connect(str(L2_DB))
truths = con.execute("SELECT * FROM dq_truths").fetchdf()         # 'truth' baseline
sql_profiles = con.execute("SELECT * FROM dq_truths_sql").fetchdf()  # SQL layer profiles
con.close()

truths = flatten_values(truths, "truth_value")
sql_profiles = flatten_values(sql_profiles, "actual_value")

truths = truths[["source", "table_name", "column_name", "metric_type", "dtype", "truth_value"]]
sql_profiles = sql_profiles[["source", "table_name", "column_name", "metric_type", "dtype", "actual_value"]]

# ----------------------------------------------------------------------------- #
# COMPARISON LOGIC
# ----------------------------------------------------------------------------- #

def compare_numeric(tv, av, tol_pct=TOLERANCE_PCT):
    try:
        t = float(tv)
        a = float(av)
    except Exception:
        return "SKIPPED", None, None
    diff = a - t
    allowed = abs(t) * tol_pct
    return ("PASS" if abs(diff) <= allowed else "FAIL", diff, allowed)

def compare_string(tv, av):
    if pd.isna(tv) and pd.isna(av):
        return "PASS"
    return "PASS" if str(tv) == str(av) else "FAIL"

def compare_pair(df_truth, df_actual, truth_name, target_table, layer):
    """Compare one truth dataset to a target table."""

    # --- Layer + Table-based exclusions ---
    excluded = EXCLUDED_COLUMNS.get(layer, {}).get(target_table, [])
    if excluded:
        before = len(df_truth)
        df_truth = df_truth[~df_truth["column_name"].isin(excluded)]
        df_actual = df_actual[~df_actual["column_name"].isin(excluded)]
        after = len(df_truth)
        print(f"ℹ️ Excluded {before - after} metrics for {target_table} ({layer}): {excluded}")

    # --- Continue as before ---
    truth_tbl = df_truth[df_truth["metric_type"].isin(TABLE_LEVEL_METRICS)].copy()
    truth_col = df_truth[~df_truth["metric_type"].isin(TABLE_LEVEL_METRICS)].copy()

    act_tbl = df_actual[df_actual["metric_type"].isin(TABLE_LEVEL_METRICS)].copy()
    act_col = df_actual[~df_actual["metric_type"].isin(TABLE_LEVEL_METRICS)].copy()

    m_tbl = pd.merge(
        truth_tbl, act_tbl,
        on=["metric_type"], how="outer", suffixes=("_truth", "_actual")
    )
    m_tbl["table_name_truth"] = truth_name
    m_tbl["table_name_actual"] = target_table

    m_col = pd.merge(
        truth_col, act_col,
        on=["column_name", "metric_type"], how="outer", suffixes=("_truth", "_actual")
    )
    m_col["table_name_truth"] = truth_name
    m_col["table_name_actual"] = target_table

    merged = pd.concat([m_tbl, m_col], ignore_index=True)

    # --- Evaluate results ---
    results = []
    for _, r in merged.iterrows():
        metric = r.get("metric_type")
        tv, av = r.get("truth_value"), r.get("actual_value")

        if metric in NUMERIC_METRICS:
            res, diff, allowed = compare_numeric(tv, av, TOLERANCE_PCT)
        else:
            res = compare_string(tv, av)
            diff, allowed = None, None

        results.append({
            "truth_source": r.get("table_name_truth"),
            "target_table": r.get("table_name_actual"),
            "layer": layer,
            "column_name": r.get("column_name"),
            "metric_type": metric,
            "dtype_truth": r.get("dtype_truth"),
            "dtype_actual": r.get("dtype_actual"),
            "truth_value": tv,
            "actual_value": av,
            "diff": diff,
            "allowed_abs": allowed,
            "tolerance_pct": TOLERANCE_PCT if metric in NUMERIC_METRICS else None,
            "result": res
        })

    return pd.DataFrame(results)


# ----------------------------------------------------------------------------- #
# MAIN
# ----------------------------------------------------------------------------- #

def main():
    all_results = []

    for link in TABLE_LINKS:
        truth_name = link["truth"]
        base_truth = truths[truths["source"] == truth_name].copy()
        if base_truth.empty:
            print(f"⚠️ No truths for {truth_name}; skipping.")
            continue

        for layer in ["L0", "L1", "L2"]:
            target_tbl = link.get(layer)
            if not target_tbl:
                continue

            actual = sql_profiles[sql_profiles["table_name"] == target_tbl].copy()
            if actual.empty:
                print(f"⚠️ No profiled rows for {target_tbl} ({layer}); skipping.")
                continue

            res = compare_pair(base_truth, actual, truth_name, target_tbl, layer)
            all_results.append(res)

    if not all_results:
        print("⚠️ No results generated — check mappings.")
        return

    final_df = pd.concat(all_results, ignore_index=True)

    print(f"\n✅ Comparison complete. {len(final_df):,} result rows generated.")

    con = duckdb.connect(str(L2_DB))
    con.execute(f"CREATE OR REPLACE TABLE {OUTPUT_TABLE} AS SELECT * FROM final_df")
    con.close()

    print(f"💾 Stored dq_results in {L2_DB}:{OUTPUT_TABLE}")
    print("✅ Done.")


if __name__ == "__main__":
    main()
