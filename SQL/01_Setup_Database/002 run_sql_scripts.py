import duckdb
from pathlib import Path

# === 1. Setup paths ===
BASE_DIR = Path(__file__).resolve().parent.parent
SQL_DIR = BASE_DIR / "sql"
DB_PATH = BASE_DIR / "SQL/forecast_accuracy.duckdb"

# === 2. List SQL scripts in the order you want them executed ===
SQL_FILES = [
    "010 create_duckdb.sql",
    "02_transform_sales.sql",
    "03_build_fact_table.sql"
]

# === 3. Connect to your database ===
con = duckdb.connect(DB_PATH)
print(f"Connected to {DB_PATH.name}")

# === 4. Loop through and execute each SQL file ===
def run_sql_scripts(SQL_FILES,SQL_DIR,BASE_DIR):
    for file_name in SQL_FILES:
        path = SQL_DIR / file_name
        print(f"\n▶ Running {path.name} ...")

        try:
            sql_script = path.read_text()
            # Replace relative 'data/' paths with absolute paths
            sql_script = sql_script.replace("data/", f"{BASE_DIR / 'data'}/")
            con.execute(sql_script)
            print(f"✅ {path.name} executed successfully")
        except Exception as e:
            print(f"❌ Error in {path.name}: {e}")

run_sql_scripts(['02 select_all.sql'],SQL_DIR=SQL_DIR,BASE_DIR=BASE_DIR)

con.close()
print("\n🏁 All scripts completed successfully!")
