# pubmed_bioner_windows.py
import os
import gzip
import json
import xml.etree.ElementTree as ET
import pandas as pd
import spacy
from sentence_splitter import SentenceSplitter
from concurrent.futures import ProcessPoolExecutor
import numpy as np
import re
import h5py
import warnings
warnings.simplefilter("ignore", FutureWarning)

# -----------------------------
# CONFIG
# -----------------------------
DATA_FOLDER = "Data"
OUT_FILE = "Result-v5/results_bioner.csv"
MODEL_NAME = "en_ner_jnlpba_md"
PROTEIN_LIST = "proteins_original.txt"
SIM_THRESHOLD = 0.85
N_WORKERS = None
EMBED_CACHE_FILE = "embed_cache.h5"
EMBED_INDEX_FILE = "embed_index.json"
MIN_TOKEN_OVERLAP = 0.7 

# -----------------------------
# HELPERS
# -----------------------------
def normalize(s):
    if not s:
        return ""
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9\- ]+", "", s)
    s = re.sub(r"\s+", " ", s)
    return s

def cosine_safe(a, b):
    na = np.linalg.norm(a)
    nb = np.linalg.norm(b)
    if na == 0 or nb == 0:
        return 0.0
    return float(np.dot(a, b) / (na * nb))

def get_span_vector(span, nlp):
    vecs = [t.vector for t in span if t.has_vector]
    if not vecs:
        return np.zeros((nlp.vocab.vectors_length,), dtype=np.float32)
    vecs = np.array(vecs)
    if vecs.ndim == 1:
        vecs = vecs.reshape(1, -1)
    return vecs.mean(axis=0).astype(np.float32)

def token_overlap_ratio(candidate, sentence):
    cand_tokens = normalize(candidate).split()
    sent_tokens = normalize(sentence).split()
    overlap = sum(1 for t in cand_tokens if t in sent_tokens)
    return overlap / max(len(cand_tokens), 1)

# -----------------------------
# EMBEDDING CACHE (main process)
# -----------------------------
def ensure_cache():
    if not os.path.exists(EMBED_CACHE_FILE):
        with h5py.File(EMBED_CACHE_FILE, "w") as f:
            f.create_group("tokens")
    if not os.path.exists(EMBED_INDEX_FILE):
        with open(EMBED_INDEX_FILE, "w") as f:
            json.dump({}, f)

def load_index():
    with open(EMBED_INDEX_FILE, "r") as f:
        return json.load(f)

def save_index(idx):
    with open(EMBED_INDEX_FILE, "w") as f:
        json.dump(idx, f)

def merge_worker_cache(worker_cache):
    """Merge local worker embeddings into main HDF5 safely."""
    idx = load_index()
    with h5py.File(EMBED_CACHE_FILE, "a") as f:
        grp = f["tokens"]
        for key, vec in worker_cache.items():
            if key in idx:
                continue
            dsname = f"v{len(idx):09d}"
            try:
                grp.create_dataset(dsname, data=vec.astype(np.float32), compression="gzip")
                idx[key] = dsname
            except Exception as e:
                print(f"Error writing {key}: {e}")
    save_index(idx)

# -----------------------------
# Load canonical proteins & embeddings
# -----------------------------

nlp = None
splitter = None
protein_embeddings = None
protein_token_sets = None

def worker_init():
    """Initializer for ProcessPoolExecutor"""
    global nlp, splitter, protein_embeddings, protein_token_sets
    nlp = spacy.load(MODEL_NAME)
    splitter = SentenceSplitter(language="en")

    # Precompute protein embeddings
    protein_embeddings = {}
    protein_token_sets = {}
    with open(PROTEIN_LIST, "r", encoding="utf8") as f:
        for line in f:
            name = line.strip()
            if not name:
                continue
            doc = nlp(name)
            vecs = [t.vector for t in doc if t.has_vector]
            if vecs:
                protein_embeddings[name] = np.array(vecs).mean(axis=0).astype(np.float32)
            else:
                protein_embeddings[name] = np.zeros((nlp.vocab.vectors_length,), dtype=np.float32)
            protein_token_sets[name] = set(normalize(name).split())
    print(f"[Worker] Loaded {len(protein_embeddings)} proteins")

# def load_protein_embeddings(nlp):
#     embeddings = {}
#     with open(PROTEIN_LIST, "r", encoding="utf8") as f:
#         for line in f:
#             name = line.strip()
#             if not name:
#                 continue
#             doc = nlp(name)
#             vecs = [t.vector for t in doc if t.has_vector]
#             if vecs:
#                 embeddings[name] = np.array(vecs).mean(axis=0).astype(np.float32)
#             else:
#                 embeddings[name] = np.zeros((nlp.vocab.vectors_length,), dtype=np.float32)
#     return embeddings

# -----------------------------
# WORKER
# -----------------------------
def process_pubmed_file(file_path: str):
    """Processes a single PubMed XML file."""
    worker_cache = {}
    results = []

    filename = os.path.basename(file_path)
    try:
        with gzip.open(file_path, "rt", encoding="utf-8") as f:
            try:
                tree = ET.parse(f)
                root = tree.getroot()
            except Exception:
                print(f"[WARN] Corrupted XML {filename}, skipping.")
                return [], {}

            for article in root.findall(".//PubmedArticle"):
                pmid = article.findtext(".//ArticleId[@IdType='pubmed']")
                if not pmid:
                    continue
                abstracts = [abst.text for abst in article.findall(".//Abstract/AbstractText") if abst.text]
                if not abstracts:
                    continue
                abstract_text = " ".join(abstracts)
                sentences = splitter.split(abstract_text)

                # Batch process sentences with SpaCy
                docs = list(nlp.pipe(sentences, batch_size=50))
                for sent, doc in zip(sentences, docs):
                    protein_entities = [e for e in doc.ents if e.label_ in {"GENE_OR_GENE_PRODUCT", "PROTEIN", "PROTEIN_COMPLEX"}]
                    matched_proteins = set()

                    for ent in protein_entities:
                        key = normalize(ent.text)
                        vec = worker_cache.get(key)
                        if vec is None:
                            vec = get_span_vector(ent, nlp)
                            worker_cache[key] = vec

                        for canon_name, canon_vec in protein_embeddings.items():
                            if cosine_safe(vec, canon_vec) >= SIM_THRESHOLD:
                                # token overlap filtering
                                if len(protein_token_sets[canon_name] & set(normalize(sent).split())) / max(len(protein_token_sets[canon_name]),1) >= MIN_TOKEN_OVERLAP:
                                    matched_proteins.add(canon_name)

                    if len(matched_proteins) >= 2:
                        results.append({
                            "PubMedID": pmid,
                            "Matched_Proteins": "; ".join(sorted(matched_proteins)),
                            "Relevant_Sentence": sent
                        })

    except Exception as e:
        print(f"[ERROR] {filename}: {e}")
        return [], {}

    # Save partial results
    if results:
        partial_file = os.path.join("Result-v5", f"{filename.replace('.xml.gz','')}.csv")
        os.makedirs(os.path.dirname(partial_file), exist_ok=True)
        pd.DataFrame(results).to_csv(partial_file, index=False)
        print(f"[INFO] Saved partial results {partial_file}")

    print(f"[INFO] Finished {filename}: {len(results)} matches")
    return results, worker_cache

# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    ensure_cache()
    files = [os.path.join(DATA_FOLDER, f) for f in os.listdir(DATA_FOLDER) if f.endswith(".gz")]
    all_rows = []
    all_worker_caches = []

    with ProcessPoolExecutor(max_workers=N_WORKERS, initializer=worker_init) as exe:
        futures = [exe.submit(process_pubmed_file, f) for f in files]
        for f in futures:
            try:
                rows, cache = f.result()
                if rows:
                    all_rows.extend(rows)
                if cache:
                    all_worker_caches.append(cache)
            except Exception as e:
                print(f"[Worker exception] {e}")

    # Merge worker caches into HDF5 safely
    for cache in all_worker_caches:
        merge_worker_cache(cache)

    # Final CSV
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    df = pd.DataFrame(all_rows)
    df.to_csv(OUT_FILE, index=False)
    print(f"\n===================================")
    print(f"DONE. Extracted {len(df)} rows")
    print(f"Saved to {OUT_FILE}")
    print(f"===================================")