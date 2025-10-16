import duckdb
from pathlib import Path

# Base project directory (one level up from /SQL)
base_dir = Path(__file__).resolve().parent.parent

# Connect to database in project root
con = duckdb.connect(base_dir / 'sql/L0 forecast_accuracy.duckdb')

# Read the SQL
sql_file = base_dir / 'SQL/010 create_duckdb.sql'
sql_script = sql_file.read_text()

# Replace relative paths inside SQL with full ones
sql_script = sql_script.replace('data/', f'{base_dir / "data"}/')

# Execute
con.execute(sql_script)
print(con.sql("SHOW TABLES;").df())

con.close()