import bz2
import os
import xml.etree.ElementTree as ET
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

# =========================
# CONFIGURATION
# =========================

PROJECT_ROOT = r"D:\Nirmit\Amazon_Product_Reviews"
RAW_FILE = r"D:\Nirmit\Amazon_Product_Reviews\data\raw\enwiki_articles.xml.bz2"
print("RAW FILE EXISTS:", os.path.exists(RAW_FILE))
OUT_DIR = os.path.join(PROJECT_ROOT, "data", "staging", "wiki_articles")

BATCH_SIZE = 1000   # change later if needed

# =========================
# DEBUG PRINTS (IMPORTANT)
# =========================

print("SCRIPT RUNNING FROM:", os.getcwd())
print("RAW FILE:", RAW_FILE)
print("OUTPUT DIR:", OUT_DIR)

# =========================
# SAFETY CHECKS
# =========================

if not os.path.exists(RAW_FILE):
    raise FileNotFoundError(f"Raw file not found: {RAW_FILE}")

os.makedirs(OUT_DIR, exist_ok=True)

# =========================
# HELPERS
# =========================

def strip_ns(tag: str) -> str:
    """Remove XML namespace"""
    return tag.split("}", 1)[-1]

# =========================
# ETL PROCESS
# =========================

rows = []
batch_id = 0

with bz2.open(RAW_FILE, "rb") as f:
    context = ET.iterparse(f, events=("end",))

    for _, elem in tqdm(context, desc="Parsing Wikipedia"):

        if strip_ns(elem.tag) != "page":
            continue

        title = None
        namespace = None
        timestamp = None
        text = None

        for child in elem:
            tag = strip_ns(child.tag)

            if tag == "title":
                title = child.text

            elif tag == "ns":
                namespace = child.text

            elif tag == "revision":
                for r in child:
                    rtag = strip_ns(r.tag)
                    if rtag == "timestamp":
                        timestamp = r.text
                    elif rtag == "text":
                        text = r.text

        rows.append({
            "title": title,
            "namespace": namespace,
            "timestamp": timestamp,
            "text": text
        })

        elem.clear()

        # =========================
        # WRITE BATCH
        # =========================
        if len(rows) >= BATCH_SIZE:
            print(f"\nWRITING BATCH {batch_id} WITH {len(rows)} ROWS")

            df = pd.DataFrame(rows)
            table = pa.Table.from_pandas(df, preserve_index=False)

            out_file = os.path.join(
                OUT_DIR,
                f"wiki_batch_{batch_id}.parquet"
            )

            pq.write_table(table, out_file)

            print(f"BATCH WRITTEN: {out_file}")

            batch_id += 1
            rows.clear()

    # =========================
    # FINAL PARTIAL BATCH
    # =========================
    if rows:
        print(f"\nWRITING FINAL BATCH {batch_id} WITH {len(rows)} ROWS")

        df = pd.DataFrame(rows)
        table = pa.Table.from_pandas(df, preserve_index=False)

        out_file = os.path.join(
            OUT_DIR,
            f"wiki_batch_{batch_id}.parquet"
        )

        pq.write_table(table, out_file)

        print(f"FINAL BATCH WRITTEN: {out_file}")

print("\nETL COMPLETED SUCCESSFULLY")
