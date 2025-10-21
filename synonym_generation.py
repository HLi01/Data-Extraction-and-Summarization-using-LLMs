import requests
import csv
import time
import pandas as pd

# Full protein list
proteins = [
    "Interleukin-18",
    "Hepatocyte growth factor",
    "C-C motif chemokine 19",
    "C-C motif chemokine 2",
    "Macrophage metalloelastase",
    "Lymphotoxin-alpha",
    "Fms-related tyrosine kinase 3 ligand",
    "Tumor necrosis factor",
    "Interleukin-17A",
    "Interleukin-2",
    "Interleukin-17F",
    "Granulocyte colony-stimulating factor",
    "Interleukin-1 beta",
    "Oxidized low-density lipoprotein receptor 1",
    "Tumor necrosis factor ligand superfamily member 12",
    "C-X-C motif chemokine 10",
    "Vascular endothelial growth factor A",
    "Interleukin-33",
    "Thymic stromal lymphopoietin",
    "Interferon gamma",
    "C-C motif chemokine 4",
    "Protransforming growth factor alpha",
    "Interleukin-13",
    "Interleukin-8",
    "C-C motif chemokine 8",
    "Interleukin-6",
    "C-C motif chemokine 13",
    "Granulocyte-macrophage colony-stimulating factor",
    "C-C motif chemokine 7",
    "Interleukin-4",
    "Tumor necrosis factor ligand superfamily member 10",
    "Oncostatin-M",
    "Interstitial collagenase",
    "Pro-epidermal growth factor",
    "Interleukin-7",
    "Interleukin-15",
    "Macrophage colony-stimulating factor 1",
    "C-X-C motif chemokine 9",
    "C-X-C motif chemokine 11",
    "Interleukin-17C",
    "Stromal cell-derived factor 1",
    "Eotaxin",
    "Interleukin-10",
    "C-C motif chemokine 3",
    "Interleukin-27",
]

HGNC_ROOT = "https://rest.genenames.org/fetch/symbol/{}"
HEADERS = {"Accept": "application/json"}

output_rows = []

# Simple mapping from protein names to common gene symbols for HGNC queries
protein_to_symbol = {
    "Interleukin-18": "IL18",
    "Hepatocyte growth factor": "HGF",
    "C-C motif chemokine 19": "CCL19",
    "C-C motif chemokine 2": "CCL2",
    "Macrophage metalloelastase": "MMP12",
    "Lymphotoxin-alpha": "LTA",
    "Fms-related tyrosine kinase 3 ligand": "FLT3LG",
    "Tumor necrosis factor": "TNF",
    "Interleukin-17A": "IL17A",
    "Interleukin-2": "IL2",
    "Interleukin-17F": "IL17F",
    "Granulocyte colony-stimulating factor": "CSF3",
    "Interleukin-1 beta": "IL1B",
    "Oxidized low-density lipoprotein receptor 1": "OLR1",
    "Tumor necrosis factor ligand superfamily member 12": "TNFSF12",
    "C-X-C motif chemokine 10": "CXCL10",
    "Vascular endothelial growth factor A": "VEGFA",
    "Interleukin-33": "IL33",
    "Thymic stromal lymphopoietin": "TSLP",
    "Interferon gamma": "IFNG",
    "C-C motif chemokine 4": "CCL4",
    "Protransforming growth factor alpha": "TGFA",
    "Interleukin-13": "IL13",
    "Interleukin-8": "IL8",
    "C-C motif chemokine 8": "CCL8",
    "Interleukin-6": "IL6",
    "C-C motif chemokine 13": "CCL13",
    "Granulocyte-macrophage colony-stimulating factor": "CSF2",
    "C-C motif chemokine 7": "CCL7",
    "Interleukin-4": "IL4",
    "Tumor necrosis factor ligand superfamily member 10": "TNFSF10",
    "Oncostatin-M": "OSM",
    "Interstitial collagenase": "MMP1",
    "Pro-epidermal growth factor": "EGF",
    "Interleukin-7": "IL7",
    "Interleukin-15": "IL15",
    "Macrophage colony-stimulating factor 1": "CSF1",
    "C-X-C motif chemokine 9": "CXCL9",
    "C-X-C motif chemokine 11": "CXCL11",
    "Interleukin-17C": "IL17C",
    "Stromal cell-derived factor 1": "CXCL12",
    "Eotaxin": "CCL11",
    "Interleukin-10": "IL10",
    "C-C motif chemokine 3": "CCL3",
    "Interleukin-27": "IL27",
}

il8_synonyms = [
    "Interleukin-8",
    "IL8",
    "C-X-C motif chemokine 8",
    "Chemokine (C-X-C motif) ligand 8",
    "Emoctakin",
    "Granulocyte chemotactic protein 1",
    "Monocyte-derived neutrophil chemotactic factor",
    "Monocyte-derived neutrophil-activating peptide",
    "Neutrophil-activating protein 1",
    "Protein 3-10C",
    "T-cell chemotactic factor"
]

for prot in proteins:
    symbol = protein_to_symbol[prot]
    url = HGNC_ROOT.format(symbol)
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code != 200:
        print(f"⚠️ Failed for {prot} ({symbol}) - status {resp.status_code}")
        continue

    data = resp.json()
    docs = data.get("response", {}).get("docs", [])
    if not docs:
        print(f"⚠️ No record for {prot} ({symbol})")
        continue

    doc = docs[0]
    alias_symbols = doc.get("alias_symbol", [])
    previous_symbols = doc.get("prev_symbol", [])
    alias_names = doc.get("alias_name", [])
    synonyms = set(alias_symbols + previous_symbols + alias_names)
    synonyms.add(prot)  # include canonical name

    output_rows.append([prot, "; ".join(sorted(synonyms))])
    time.sleep(0.2)  # avoid server overload

output_rows.append(["Interleukin-8", "; ".join(il8_synonyms)])
# Save to CSV
with open("protein_synonyms.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["Protein", "Synonyms"])
    for row in output_rows:
        writer.writerow(row)

print("✅ Synonym list saved to protein_synonyms_only.csv")

df = pd.read_csv("protein_synonyms.csv")

# Count how many synonyms each protein has
df["Synonym_Count"] = df["Synonyms"].apply(lambda x: len([s.strip() for s in x.split(";")]))

# Print a quick summary
print("\nProtein synonym statistics:")
print(df[["Protein", "Synonym_Count"]].sort_values(by="Synonym_Count", ascending=False).to_string(index=False))

# Optional: overall stats
total_proteins = len(df)
avg_synonyms = df["Synonym_Count"].mean()
max_synonyms = df["Synonym_Count"].max()
min_synonyms = df["Synonym_Count"].min()

print(f"\nTotal proteins: {total_proteins}")
print(f"Average synonyms per protein: {avg_synonyms:.2f}")
print(f"Maximum synonyms for a protein: {max_synonyms}")
print(f"Minimum synonyms for a protein: {min_synonyms}")


