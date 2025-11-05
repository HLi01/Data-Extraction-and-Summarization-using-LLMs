import os
import re
import requests
from urllib.parse import urljoin
from pathlib import Path
import sys
import gzip
import tempfile


URL = "https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/"
TARGET_DIR = Path("Data")
TARGET_DIR.mkdir(exist_ok=True)

# Safety defaults — set to True if you want to actually download
DOWNLOAD = False        # change to True to download

# requests session with a safe fallback if certs fail
session = requests.Session()
def get(url):
    """Get URL with files to extract."""
    try:
        r = session.get(url, timeout=30)
        r.raise_for_status() # will raise an error if the request failed
        return r
    except requests.exceptions.SSLError as e:
        print("SSL error when fetching", url, "\nRetrying with certificate verification disabled.")
        r = session.get(url, timeout=30, verify=False)
        r.raise_for_status()
        return r


href_re = re.compile(r'href="([^"]+)"', flags=re.IGNORECASE) # to find href links

def list_tarballs_in_url(url):
    """Returns a list of all .tar.gz files found at the given URL."""
    r = get(url)
    text = r.text
    names = href_re.findall(text)
    # filter xml.gz files:
    xmlfiles = []
    for n in names:
        n = n.strip()
        if n.endswith(".xml.gz"):
            # try to extract size / date from the text line for human info
            xmlfiles.append(n)
    return xmlfiles


def download_file(url, dest: Path):
    """Download a file from URL to destination path."""
    print("Downloading", url, "->", dest)
    with session.get(url, stream=True, timeout=60, verify=False) as r:
        r.raise_for_status()
        with open(dest, "wb") as fh:
            for chunk in r.iter_content(chunk_size=1024*1024):
                if chunk:
                    fh.write(chunk)


def open_xml_gz(files):
    """Generator that yields file handles for opened .xml.gz files."""
    for f in files:
        with gzip.open(f, 'rt', encoding='utf-8') as fh:
            yield fh


def compare_and_merge(originals, updates):
    """Compare the original and updated files, replacing originals with updates where available and adding new files."""
    update_dict = {os.path.basename(u): u for u in updates}
    originals_dict = {os.path.basename(o): o for o in originals}

    # Replace originals with updates where available
    final_files = [update_dict.get(name, orig) for name, orig in originals_dict.items()]

    # Add new files
    new_files = [
        u for name, u in update_dict.items() if name not in originals_dict
    ]

    return final_files + new_files


def download_files(files):
    for file_path in files:
        with gzip.open(file_path, "rt", encoding="utf-8") as f:
            content = f.read()

        with gzip.open(file_path, 'wt', encoding="utf-8") as f:
            f.write(content)


def main():
    print("URL where all articles are:", URL)
    try:
        update_files = list_tarballs_in_url(URL)
    except Exception as e:
        print("  ERROR listing", URL, ":", e)
    if not update_files:
        print("  (no .tar.gz found in)", URL)
    print(f"  Found {len(update_files)} .xml.gz in the directory")

    data_folder = "Data"
    original_files = [os.path.join(data_folder, f) for f in os.listdir(data_folder) if f.endswith(".gz")]
    final_files = compare_and_merge(original_files, update_files)

    download_files(final_files)
    

if __name__ == "__main__":
    main()