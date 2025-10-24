import duckdb
import pandas as pd
from pathlib import Path

# ---------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------
DB_PATH = "SQL/L2_business_insights.duckdb"
SKU_SAMPLE_PATH = Path("SQL/outputs/sku_sample.csv")
SAMPLE_RATE = 1  # 0.5%


# ---------------------------------------------------------------------
# SAMPLE (OPTIONAL) — RUN ONCE TO CREATE SKU LIST
# ---------------------------------------------------------------------
def sample_skus(con, sample_rate=0.005, save=True):
    """Randomly sample SKUs and save to CSV (for consistent future use)."""

    df = con.execute(f"""
        SELECT product_key, item_id, dept_id, cat_id
        FROM L2_business_insights.dim_product
        WHERE random() < {sample_rate}
    """).fetchdf()

    print(f"✅ Sampled {len(df):,} SKUs from dim_product")

    if save:
        SKU_SAMPLE_PATH.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(SKU_SAMPLE_PATH, index=False)
        print(f"💾 Saved sampled SKUs to {SKU_SAMPLE_PATH}")

    return df

# DB_PATH = "SQL/L2_business_insights.duckdb"
# TABLE_NAME = "l2_sales_long_extended"


# con = duckdb.connect(DB_PATH)

# sample_skus(con,1,True)

# ---------------------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------------------
def main(get_sample: bool = False, sample_path=SKU_SAMPLE_PATH):
    """Pull joined sales/calendar/product data for sampled SKUs."""
    con = duckdb.connect(DB_PATH)

    if get_sample or not sample_path.exists():
        sku_sample_df = sample_skus(con, SAMPLE_RATE)
    else:
        sku_sample_df = pd.read_csv(sample_path)
        print(f"📂 Loaded existing SKU sample ({len(sku_sample_df):,} SKUs)")

    # Register sample as a temp table
    con.register("sku_sample", sku_sample_df)

    # -----------------------------------------------------------------
    # Pull joined dataset
    # -----------------------------------------------------------------
    query = """
    SELECT
        f.date_key,
        f.product_key,
        f.store_key,
        f.sales,
        f.sell_price,
        f.sales_value,
        c.date,
        c.year,
        c.month,
        c.wm_yr_wk,
        c.wday,
        c.weekday,
        p.item_id,
        p.dept_id,
        p.cat_id
    FROM L2_business_insights.l2_sales_long_extended AS f
    JOIN sku_sample AS p
      ON f.product_key = p.product_key
    LEFT JOIN L2_business_insights.dim_calendar AS c
      ON f.date_key = c.date_key
    """

    df = con.execute(query).fetchdf()
    con.close()

    print(f"✅ Pulled {len(df):,} rows for sampled SKUs")

    # Ensure date is proper datetime
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Save and return
    out_path = Path("data/forecast_training_sample.parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    print(f"💾 Saved joined dataset → {out_path}")

    return df
