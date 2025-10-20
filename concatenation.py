import os
import pandas as pd

result_folder = "Result"
output_file = os.path.join(result_folder, "all_results_concatenated.csv")

# Collect all CSV files
csv_files = [os.path.join(result_folder, f) for f in os.listdir(result_folder) if f.endswith("_filtered.csv")]

if not csv_files:
    print("No filtered CSV files found in the Result folder.")
else:
    print(f"Found {len(csv_files)} CSV files. Concatenating...")

    # Load all CSVs into DataFrames
    dfs = []
    for file in csv_files:
        try:
            df = pd.read_csv(file)
            dfs.append(df)
            print(f"Loaded {os.path.basename(file)} ({len(df)} rows)")
        except Exception as e:
            print(f"Error reading {file}: {e}")

    # Concatenate them into one DataFrame
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)

        # Reorder columns
        expected_columns = ["PubMedID", "Matched_Chemicals", "Abstract"]
        for col in expected_columns:
            if col not in combined_df.columns:
                combined_df[col] = ""
        combined_df = combined_df[expected_columns]

        # Save combined file
        combined_df.to_csv(output_file, index=False)
        print(f"\nCombined CSV saved to: {output_file}")
        print(f"Total rows: {len(combined_df)}")
    else:
        print("No valid dataframes to concatenate.")
