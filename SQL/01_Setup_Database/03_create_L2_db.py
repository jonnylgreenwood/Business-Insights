import duckdb
from pathlib import Path

# Base project directory (one level up from /SQL)
base_dir = Path(__file__).resolve().parent.parent.parent

# Connect to database in project root
con = duckdb.connect(base_dir / 'SQL/L2_business_insights.duckdb')

con.close()