import polars as pl

input_path = "Result-v4/all_results_concatenated.csv"
output_path = "Result-v4/all_results_cleaned.csv"

df = pl.read_csv(input_path)

print(f"Loaded dataset with {df.height} rows and {df.width} columns")
original_length = df.height

# Trim whitespace from all columns
df = df.with_columns([
    pl.col(col).str.strip_chars() if df[col].dtype == pl.Utf8 else pl.col(col)
    for col in df.columns
])

# Replace empty strings or "NA" / "None" with nulls
df = df.with_columns([
    pl.when(pl.col(col).str.to_lowercase().is_in(["", "NA", "N/A", "None", "null"]))
    .then(None)
    .otherwise(pl.col(col))
    .alias(col)
    for col in df.columns if df[col].dtype == pl.Utf8
])

print(f"Number of empty strings: {original_length-df.height}")

# Drop rows where essential columns are missing
essential_cols = ["PubMedID", "Relevant_Sentences"]
df = df.drop_nulls(subset=essential_cols)

# Normalize spacing and punctuation in "Relevant_Sentences"
df = df.with_columns(
    pl.col("Relevant_Sentences")
    .str.replace_all(r"\s*\|\|\s*", " || ")  # ensure consistent separator
    .str.replace_all(r"\s{2,}", " ")         # collapse extra spaces
    .str.strip_chars()                       # trim edges
    .alias("Relevant_Sentences")
)

# Remove duplicate rows (exact duplicates)
df = df.unique(subset=["PubMedID", "Matched_Proteins", "Relevant_Sentences"])

# Save cleaned dataset
df.write_csv(output_path)
print(f"Cleaned dataset saved to '{output_path}' with {df.height} rows")
print(f"Cleaned dataset size: {df.height}")
print("\nExample cleaned rows:")
print(df.head(5))

