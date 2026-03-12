import pandas as pd
import dask.dataframe as dd
import gzip
import shutil
import os
import time

# مسار الملف
file_path = r"C:\Users\BZ BOOK\Desktop\train.csv"

print("=================================================")
print("DATASET:", file_path)
print("File size:", round(os.path.getsize(file_path)/(1024*1024),2), "MB")
print("=================================================")


# ======================================
# Method 1 : Pandas with chunks
# ======================================

print("\nMethod 1 : Pandas with chunks")

start = time.time()

rows = 0
for chunk in pd.read_csv(file_path, chunksize=1000000):
    rows += len(chunk)

end = time.time()

print("Total rows:", rows)
print("Execution time:", round(end-start,2), "seconds")


# ======================================
# Method 2 : Dask
# ======================================

print("\nMethod 2 : Dask")

start = time.time()

df = dd.read_csv(file_path)

rows = df.shape[0].compute()

end = time.time()

print("Total rows:", rows)
print("Execution time:", round(end-start,2), "seconds")


# ======================================
# Method 3 : Compression (whole file)
# ======================================

print("\nMethod 3 : Compression ")

compressed_file = r"C:\Users\BZ BOOK\Desktop\train_compressed.csv.gz"

start = time.time()

# ضغط الملف كامل مرة واحدة
with open(file_path, "rb") as f_in:
    with gzip.open(compressed_file, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)

end = time.time()

print("Compression time:", round(end-start,2), "seconds")

original_size = os.path.getsize(file_path)/(1024*1024)
compressed_size = os.path.getsize(compressed_file)/(1024*1024)

print("Original size:", round(original_size,2), "MB")


print("\n=================================================")
print("Comparison Finished")
print("=================================================")