import glob
import pyarrow.parquet as pq

files = glob.glob(r"data\staging\wiki_articles\*.parquet")

print("Parquet file count:", len(files))

pf = pq.ParquetFile(files[0])
print("Schema:")
print(pf.schema)

print("Parquet files are valid")
