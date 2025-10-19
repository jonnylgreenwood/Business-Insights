import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path

# === CONFIG ===
input_path = Path("SQL/outputs/parquet/l2_sales_long.parquet")
rows_per_file = 1_000_000  # adjust as needed for Power BI comfort
output_dir = input_path.parent / "splits"  # creates SQL/outputs/parquet/splits
output_prefix = "l2_sales_long_part"

# === SETUP ===
output_dir.mkdir(exist_ok=True)
reader = pq.ParquetFile(input_path)

# === PROCESS ===
batch_index = 1
for batch in reader.iter_batches(batch_size=rows_per_file):
    table = pa.Table.from_batches([batch])
    output_file = output_dir / f"{output_prefix}_{batch_index}.parquet"
    pq.write_table(table, output_file)
    print(f"✅ Wrote {output_file} ({table.num_rows:,} rows)")
    batch_index += 1

print(f"🎉 Done! Created {batch_index-1} split files in {output_dir}")
