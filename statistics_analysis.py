import polars as pl
import re
from collections import OrderedDict

# --- Paths ---
csv_path = "Result-v3/all_results_cleaned.csv"

# --- Protein dictionary ---
protein_info = OrderedDict({
    "Interleukin-18": ["Interleukin-18", "IL-18", "IL18"],
    "Hepatocyte growth factor": ["Hepatocyte growth factor", "HGF"],
    "C-C motif chemokine 19": ["C-C motif chemokine 19", "CCL19"],
    "C-C motif chemokine 2": ["C-C motif chemokine 2", "CCL2", "MCP-1", "MCP1"],
    "Macrophage metalloelastase": ["Macrophage metalloelastase", "MMP12"],
    "Lymphotoxin-alpha": ["Lymphotoxin-alpha", "LTA"],
    "Tumor necrosis factor": ["Tumor necrosis factor", "TNF", "TNFα", "TNF-alpha"],
    "Interleukin-17A": ["Interleukin-17A", "Interleukin 17A", "IL-17A", "IL17A"],
    "Interleukin-17F": ["Interleukin-17F", "Interleukin 17F", "IL-17F", "IL17F"],
    "Interleukin-17C": ["Interleukin-17C", "Interleukin 17C", "IL-17C", "IL17C"],
    "C-X-C motif chemokine 10": ["C-X-C motif chemokine 10", "CXCL10", "IP-10", "IP10"],
    "Interferon gamma": ["Interferon gamma", "IFN-gamma", "IFNγ", "IFNG"],
    "Interleukin-6": ["Interleukin-6", "IL-6", "IL6"],
    "Vascular endothelial growth factor A": ["Vascular endothelial growth factor A", "VEGFA", "VEGF-A", "VEGF"],
    "Interleukin-8": ["Interleukin-8", "IL-8", "IL8", "CXCL8"],
    "Interleukin-10": ["Interleukin-10", "IL-10", "IL10"],
    "Interleukin-4": ["Interleukin-4", "IL-4", "IL4"],
    "C-X-C motif chemokine 9": ["C-X-C motif chemokine 9", "CXCL9"],
    "C-X-C motif chemokine 11": ["C-X-C motif chemokine 11", "CXCL11"],
    "Oncostatin-M": ["Oncostatin-M", "OSM"],
    "Eotaxin": ["Eotaxin", "CCL11"],
})

# --- Load CSV lazily ---
df = pl.scan_csv(csv_path)

total_rows = df.select(pl.len()).collect().item()
print(f"Total rows in dataset: {total_rows}")

# Count protein occurrences
results = []
for protein, syns in protein_info.items():
    # Build regex pattern for synonyms
    escaped = []
    for s in syns:
        alt = re.escape(s).replace(r"\-", r"[-\s]?").replace(r"\ ", r"\s+")
        escaped.append(alt)
    pattern = r"\b(?:{})\b".format("|".join(escaped))
    
    # total_count = df.select(pl.sum(count_expr).alias("count")).collect()["count"][0]
    total_count = df.select(pl.col("Abstract").str.count_matches(pattern, literal=False).sum()).collect().item()
    
    res = {"Protein": protein, "Count_in_Text": total_count}
    print(f"{protein} done\n{res}")
    results.append(res)

# --- Sort results ---
res_df = pl.DataFrame(results).sort("Count_in_Text", reverse=True)

# --- Display ---
print("\nProtein occurrence counts in Abstracts):")
print(res_df)
