import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.metrics import mean_absolute_percentage_error

# 🔗 Your data-prep module (the one that samples SKUs & joins calendar/product)
import helper_forecasting_data_prep as fc  # keep this name/path consistent with your repo

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------
GET_SAMPLE = True                          # only toggle if you want to resample SKUs
SAMPLE_FILE_PATH = Path("SQL/outputs/sku_sample.csv")

# ---------------------------------------------------------------------
# 1️⃣ Load sampled & joined data from the module
# ---------------------------------------------------------------------
print("📦 Loading training sample from DuckDB via module...")
df = fc.main(get_sample=GET_SAMPLE, sample_path=SAMPLE_FILE_PATH)
print(f"✅ Loaded {len(df):,} rows")

# ---------------------------------------------------------------------
# Ensure we have a valid datetime column
# ---------------------------------------------------------------------
if "date" not in df.columns:
    # Try to infer possible date-like columns
    date_cols = [c for c in df.columns if c.lower() in ["date", "date_x", "date_y"]]
    if date_cols:
        df["date"] = pd.to_datetime(df[date_cols[0]], errors="coerce")
    elif df["date_key"].astype(str).str.match(r"^\d{8}$").all():
        df["date"] = pd.to_datetime(df["date_key"].astype(str), format="%Y%m%d", errors="coerce")
    else:
        raise ValueError("❌ No valid date column found in data.")
else:
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

# Sort for time operations
df = df.sort_values(["product_key", "store_key", "date"]).reset_index(drop=True)

# Keep only relevant columns
base_cols = ["date", "date_key", "product_key", "store_key", "sales", 'sell_price', 'sales_value']
keep_cols = [c for c in base_cols if c in df.columns]
df = df[keep_cols].copy()

# ---------------------------------------------------------------------
# 2️⃣ Forecast functions (vectorised per group, pandas-only)
# ---------------------------------------------------------------------
def make_forecasts_one_group(g: pd.DataFrame) -> pd.DataFrame:
    """
    Apply:
      - fc_naive   = y_{t-1}
      - fc_snaive  = y_{t-1y} (same calendar date last year)
      - fc_r3m     = mean of last 90 days (shifted by 1 → no peeking)
      - fc_r12m    = mean of last 365 days (shifted by 1 → no peeking)
      - fc_drift   = random walk with drift one-step-ahead
      + flag forecast window = last year of the series
    """
    g = g.sort_values("date").copy()

    # Aggregate duplicates safely (important for snaive)
    if g["date"].duplicated().any():
        g = g.groupby("date", as_index=False)["sales"].sum()

    s = g["sales"].astype(float)

    if s.notna().sum() < 100:
        return pd.DataFrame(columns=list(g.columns) + [
            "is_forecast_window", "fc_naive", "fc_snaive", "fc_r3m", "fc_r12m", "fc_drift"
        ])

    # Forecast window: last year of data
    cutoff = g["date"].max() - pd.DateOffset(years=1)
    g["is_forecast_window"] = g["date"] > cutoff

    # 1️⃣ Naïve (yesterday)
    g["fc_naive"] = s.shift(1)

    # 2️⃣ Seasonal naïve (same calendar date last year)
    s_by_date = pd.Series(s.values, index=g["date"]).groupby(level=0).mean()
    prev_year = s_by_date.to_frame("prev")
    prev_year.index = prev_year.index + pd.DateOffset(years=1)

    # Deduplicate after shifting
    prev_year = prev_year.groupby(level=0).mean()

    # Map safely by date
    g["fc_snaive"] = g["date"].map(prev_year["prev"])

    # 3️⃣ Rolling 3-month mean (≈90d), no peeking
    g["fc_r3m"] = s.rolling(window=90, min_periods=1).mean().shift(1)

    # 4️⃣ Rolling 12-month mean (≈365d), no peeking
    g["fc_r12m"] = s.rolling(window=365, min_periods=1).mean().shift(1)

    # 5️⃣ Drift (random walk with drift)
    prev = s.shift(1)
    t = np.arange(len(s), dtype=float)
    den = np.where(t > 1, t - 1, np.nan)
    y1 = float(s.iloc[0]) if len(s) else np.nan
    slope = (prev - y1) / den
    g["fc_drift"] = prev + slope

    return g


# ---------------------------------------------------------------------
# 3️⃣ Apply to all SKU×store
# ---------------------------------------------------------------------
print("⚙️ Generating forecasts across all SKU×store...")
out = (
    df.groupby(["product_key", "store_key"], group_keys=False)
      .apply(make_forecasts_one_group)
      .reset_index(drop=True)
)

if out.empty:
    raise RuntimeError("No valid series produced forecasts. Check input coverage or sales column.")

print(f"✅ Forecasts created for {out['product_key'].nunique()} SKUs and "
      f"{out[['product_key','store_key']].drop_duplicates().shape[0]} SKU×store pairs")

# ---------------------------------------------------------------------
# 4️⃣ Evaluate forecast accuracy on last-1-year window
# ---------------------------------------------------------------------
methods = ["fc_naive", "fc_snaive", "fc_r3m", "fc_r12m", "fc_drift"]
eval_df = out[out["is_forecast_window"]]

print("\n📊 Out-of-sample MAPE (last-1-year window):")
for m in methods:
    valid = eval_df[["sales", m]].dropna()
    if len(valid) > 0:
        mape = mean_absolute_percentage_error(valid["sales"], valid[m])
        print(f"{m:<10} | MAPE: {mape:.3f}")
    else:
        print(f"{m:<10} | (no valid data in window)")

import duckdb

# ---------------------------------------------------------------------
# 5️⃣ Write forecasts back to DuckDB
# ---------------------------------------------------------------------
DB_PATH = "SQL/L2_business_insights.duckdb"
TABLE_NAME = "l2_sales_long_extended"

print(f"\n💾 Writing forecast results to DuckDB → {TABLE_NAME}")

con = duckdb.connect(DB_PATH)

# Drop + recreate table for clean overwrite
con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")

# Create table directly from pandas DataFrame
con.register("forecast_df", out)
con.execute(f"CREATE TABLE {TABLE_NAME} AS SELECT * FROM forecast_df")
con.unregister("forecast_df")
con.close()

print(f"✅ Stored {len(out):,} forecast rows in {DB_PATH}:{TABLE_NAME}")
