import pandas as pd
from pathlib import Path

# Define directories
input_dir = Path("SQL/outputs/csv_for_markdown")
output_dir = Path("SQL/outputs/csv_for_markdown_complete")

# Ensure output folder exists
output_dir.mkdir(parents=True, exist_ok=True)

# Loop through all CSV files in input directory
for csv_file in input_dir.glob("*.csv"):
    try:
        # Read CSV
        df = pd.read_csv(csv_file)

        # Convert to Markdown
        md_table = df.to_markdown(index=False)

        # Define output file
        output_file = output_dir / (csv_file.stem + ".md")

        # Write Markdown
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(md_table)

        print(f"✅ Converted: {csv_file.name} → {output_file.name}")

    except Exception as e:
        print(f"⚠️ Failed to convert {csv_file.name}: {e}")

print("\n✨ All CSVs processed! Markdown files saved in:", output_dir)
