import polars as pl

df = pl.read_csv("Result-v4\\all_results_concatenated.csv")

#Split 'Relevant_Sentences' into lists
df = df.with_columns(
    pl.col("Relevant_Sentences")
    .str.split("||")
    .alias("Split_Sentences")
)

#Explode the list so each sentence becomes its own row
sentences_df = (
    df.explode("Split_Sentences")
    .select([
        pl.col("PubMedID"),
        pl.col("Matched_Proteins"),
        pl.col("Split_Sentences").str.strip_chars().alias("Relevant_Sentence")
    ]).filter(pl.col("Relevant_Sentence") != "")
)

output_file = "sentences.csv"
sentences_df.write_csv(output_file)

print(f"Saved {sentences_df.height} sentences (with PubMed IDs and Matched Proteins) to '{output_file}'")
print("\n📋 Example rows:")
print(sentences_df.head(10))
print(len(sentences_df))
