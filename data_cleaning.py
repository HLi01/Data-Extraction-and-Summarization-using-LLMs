import polars as pl
import os

input_path = "Result-v3/all_results_concatenated.csv"
output_path = "Result-v3/all_results_cleaned.csv"

os.makedirs("Result-v3", exist_ok=True)

# Lazy read CSV
df = pl.scan_csv(input_path)

# Ensure Abstract exists
if "Abstract" not in df.columns:
    df = df.with_columns(pl.lit("").alias("Abstract"))

# Lazy cleaning pipeline
df = (
    df
    .with_columns(pl.col("Abstract").str.strip_chars().alias("Abstract"))  # Clean whitespace
    .filter(~pl.col("Abstract").str.contains("This article has been retracted", literal=True))  # Remove retracted
    .filter(pl.col("Abstract").str.len_chars() > 70)  # Remove very short abstracts
)

# Write lazily to CSV without collecting
df.sink_csv(output_path)

print(f"Cleaned CSV saved to: {output_path}")
