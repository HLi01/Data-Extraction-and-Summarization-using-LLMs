import pandas as pd
import os

input_path = "Result/all_results_concatenated.csv"
output_path = "Result/all_results_cleaned.csv"

df = pd.read_csv(input_path)

# Ensure Abstract exists and clean whitespace
if "Abstract" not in df.columns:
    df["Abstract"] = ""
df["Abstract"] = df["Abstract"].astype(str).str.strip()

# Remove rows with "This article has been retracted" in the Abstract
df = df[~df["Abstract"].str.contains("This article has been retracted", case=False, na=False)]
print(f"Original rows: {len(df)}")

# Remove rows with very short abstracts
df = df[df["Abstract"].str.len() > 70]
print(f"Rows after cleaning: {len(df)}")

os.makedirs("Result", exist_ok=True)
df.to_csv(output_path, index=False)
print(f"Cleaned CSV saved to: {output_path}")
