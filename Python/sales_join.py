import duckdb
import pandas as pd
import numpy as np
from pathlib import Path

# ----------------------------------------------------------------------------- #
# CONFIGURATION
# ----------------------------------------------------------------------------- #

SQL_DIR = Path("SQL")
L2_DB = SQL_DIR / "L2_business_insights.duckdb"

# Get all duckdb tables from L2 as pd df

con = duckdb.connect(str(L2_DB))
l2_sales_long = con.execute("SELECT * FROM l2_sales_long").fetchdf()
dim_product = con.execute("SELECT * FROM dim_product").fetchdf()
dim_store = con.execute("SELECT * FROM dim_store").fetchdf()
dim_calendar = con.execute("SELECT * FROM dim_calendar").fetchdf()
con.close()

# Join by keys

df = l2_sales_long.merge(dim_product, on="product_key", how="left")
df = df.merge(dim_store, on="store_key", how="left")
df = df.merge(dim_calendar, on="date_key", how="left")

print(df.shape)
print(df.head())

# All sales for CA_2, cat_id HOBBIES in 2013-2014 on joined data

filtered = df[
    (df["store_id"] == "CA_2") &
    (df["cat_id"] == "HOBBIES") &
    (df["year"].isin([2013, 2014]))
]

total_sales = filtered['sales'].sum()
print(f"Total sales for CA_2, HOBBIES, 2013-2014: {total_sales:,.0f}")