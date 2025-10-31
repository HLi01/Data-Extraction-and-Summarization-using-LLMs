import polars as pl
import re
from collections import OrderedDict

# --- Paths ---
csv_path = "Result-v3/all_results_cleaned.csv"

# --- Protein dictionary ---

protein_info = OrderedDict({
    "Interleukin-18": [
        "igif", "il-18", "il-1g", "il1f4", "interleukin-18", "interferon-gamma-inducing factor"
    ],
    "Hepatocyte growth factor": [
        "dfnb39", "f-tcf", "hgfb", "hpta", "hepatocyte growth factor", "sf",
        "fibroblast-derived tumor cytotoxic factor", "hepatopoietin a",
        "lung fibroblast-derived mitogen", "scatter factor"
    ],
    "C-C motif chemokine 19": [
        "c-c motif chemokine 19", "cc chemokine ligand 19", "ck beta-11", "ckb11",
        "ebi1-ligand chemokine", "elc", "mip-3b", "scya19",
        "beta chemokine exodus-3", "exodus-3"
    ],
    "C-C motif chemokine 2": [
        "c-c motif chemokine 2", "gdcf-2", "hc11", "mcaf", "mcp-1", "mcp1",
        "mgc9434", "scya2", "smc-cf", "monocyte chemoattractant protein-1"
    ],
    "Macrophage metalloelastase": [
        "hme", "macrophage metalloelastase", "macrophage elastase"
    ],
    "Lymphotoxin-alpha": [
        "lt", "lymphotoxin-alpha", "tnf superfamily member 1", "tnfb", "tnfsf1"
    ],
    "Fms-related tyrosine kinase 3 ligand": [
        "fms-related tyrosine kinase 3 ligand"
    ],
    "Tumor necrosis factor": [
        "dif", "tnf superfamily, member 2", "tnf-alpha", "tnfa", "tnfsf2", "tumor necrosis factor"
    ],
    "Interleukin-17A": [
        "ctla8", "il-17", "il-17a", "il17", "interleukin-17a", "cytotoxic t-lymphocyte-associated protein 8"
    ],
    "Interleukin-2": [
        "il-2", "interleukin-2", "t cell growth factor", "tcgf"
    ],
    "Interleukin-17F": [
        "il-17f", "interleukin-17f", "ml-1", "ml1"
    ],
    "Granulocyte colony-stimulating factor": [
        "c17orf33", "g-csf", "gcsf", "granulocyte colony-stimulating factor",
        "mgc45931", "filgrastim", "granulocyte colony stimulating factor",
        "lenograstim", "pluripoietin"
    ],
    "Interleukin-1 beta": [
        "il-1b", "il1-beta", "il1f2", "interleukin-1 beta"
    ],
    "Oxidized low-density lipoprotein receptor 1": [
        "clec8a", "lox-1", "oxidized low-density lipoprotein receptor 1", "scare1"
    ],
    "Tumor necrosis factor ligand superfamily member 12": [
        "apo3l", "dr3lg", "tnf12", "tweak", "tumor necrosis factor ligand superfamily member 12"
    ],
    "C-X-C motif chemokine 10": [
        "c-x-c motif chemokine 10", "c7", "ifi10", "inp10", "ip-10",
        "scyb10", "crg-2", "gip-10", "mob-1"
    ],
    "Vascular endothelial growth factor A": [
        "vegf", "vegf-a", "vpf", "vascular endothelial growth factor a"
    ],
    "Interleukin-33": [
        "c9orf26", "dkfzp586h0523", "dvs27", "dvs27-related protein", "il1f11",
        "interleukin-33", "nf-hev", "interleukin-1 family, member 11",
        "nuclear factor for high endothelial venules"
    ],
    "Thymic stromal lymphopoietin": [
        "thymic stromal lymphopoietin", "thymic stroma-derived lymphopoietin"
    ],
    "Interferon gamma": [
        "interferon gamma"
    ],
    "C-C motif chemokine 4": [
        "at744.1", "act-2", "c-c motif chemokine 4", "lag1", "mip-1-beta", "scya4"
    ],
    "Protransforming growth factor alpha": [
        "protransforming growth factor alpha"
    ],
    "Interleukin-13": [
        "alrh", "bhr1", "bronchial hyperresponsiveness-1", "il-13", "interleukin-13",
        "mgc116786", "mgc116788", "mgc116789", "p600", "allergic rhinitis"
    ],
    "C-C motif chemokine 8": [
        "c-c motif chemokine 8", "hc14", "mcp-2", "scya8"
    ],
    "Interleukin-6": [
        "bsf2", "hgf", "hsf", "ifnb2", "il-6", "interleukin-6", "interferon, beta 2"
    ],
    "C-C motif chemokine 13": [
        "c-c motif chemokine 13", "ckb10", "mcp-4", "mgc17134", "ncc-1", "scya13", "scyl1"
    ],
    "Granulocyte-macrophage colony-stimulating factor": [
        "gm-csf", "gmcsf", "granulocyte-macrophage colony-stimulating factor",
        "granulocyte-macrophage colony stimulating factor", "molgramostim", "sargramostim"
    ],
    "C-C motif chemokine 7": [
        "c-c motif chemokine 7", "fic", "marc", "mcp-3", "mcp3", "nc28", "scya6", "scya7",
        "monocyte chemoattractant protein 3", "monocyte chemotactic protein 3"
    ],
    "Interleukin-4": [
        "b cell growth factor 1", "bcgf-1", "bcgf1", "bsf1", "b_cell stimulatory factor 1",
        "il-4", "interleukin-4", "mgc79402", "lymphocyte stimulatory factor 1"
    ],
    "Tumor necrosis factor ligand superfamily member 10": [
        "apo-2l", "cd253", "tancr", "tl2", "trail", "tumor necrosis factor ligand superfamily member 10"
    ],
    "Oncostatin-M": [
        "mgc20461", "oncostatin-m"
    ],
    "Interstitial collagenase": [
        "clg", "interstitial collagenase"
    ],
    "Pro-epidermal growth factor": [
        "pro-epidermal growth factor"
    ],
    "Interleukin-7": [
        "il-7", "interleukin-7"
    ],
    "Interleukin-15": [
        "il-15", "interleukin-15", "mgc9721"
    ],
    "Macrophage colony-stimulating factor 1": [
        "m-csf", "mcsf", "mgc31930", "macrophage colony-stimulating factor 1",
        "macrophage colony stimulating factor 1"
    ],
    "C-X-C motif chemokine 9": [
        "c-x-c motif chemokine 9", "cmk", "humig", "mig", "scyb9", "crg-10"
    ],
    "C-X-C motif chemokine 11": [
        "c-x-c motif chemokine 11", "h174", "i-tac", "ip-9", "scyb11", "scyb9b", "b-r1"
    ],
    "Interleukin-17C": [
        "cx2", "il-17c", "il-21", "interleukin-17c", "mgc126884", "mgc138401"
    ],
    "Stromal cell-derived factor 1": [
        "pbsf", "scyb12", "sdf-1a", "sdf-1b", "sdf1", "sdf1a", "sdf1b",
        "stromal cell-derived factor 1", "tlsf-a", "tlsf-b"
    ],
    "Eotaxin": [
        "eotaxin", "mgc22554", "scya11", "eotaxin-1"
    ],
    "Interleukin-10": [
        "csif", "il-10", "il10a", "interleukin-10",
        "t-cell growth inhibitory factor", "tgif", "cytokine synthesis inhibitory factor"
    ],
    "C-C motif chemokine 3": [
        "c-c motif chemokine 3", "g0s19-1", "ld78", "ld78alpha", "mip-1-alpha", "sci", "scya3",
        "macrophage inflammatory protein 1 alpha"
    ],
    "Interleukin-27": [
        "il-27", "il-27a", "il27a", "il27p28", "il30", "interleukin-27", "mgc71873", "p28"
    ],
    "Interleukin-8": [
        "interleukin-8", "il8", "c-x-c motif chemokine 8",
        "chemokine (c-x-c motif) ligand 8", "emoctakin",
        "granulocyte chemotactic protein 1",
        "monocyte-derived neutrophil chemotactic factor",
        "monocyte-derived neutrophil-activating peptide",
        "neutrophil-activating protein 1", "protein 3-10c"
    ]
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
    # Add case-insensitive flag (?i)
    pattern = r"(?i)\b(?:{})\b".format("|".join(escaped))
    
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
