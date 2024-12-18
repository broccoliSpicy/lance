import datetime
import os
import pyarrow.parquet as pq
from lance.file import LanceFileReader, LanceFileWriter
import numpy as np
import pandas as pd


# Function to get file size in human-readable format
def get_file_size(file_path):
    size_bytes = os.path.getsize(file_path)
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} TB"

# Parquet file setup
parquet_file = "/home/x/internship_summary/column_0.parquet"
lance_file = "/home/x/internship_summary/column_0.lance"

# Randomly sample indices
num_rows_to_sample = 100
total_rows_num = 3213835
random_indices = np.random.choice(total_rows_num, size=num_rows_to_sample, replace=False)
random_indices.sort()

# Measure Parquet read time
start = datetime.datetime.now()
tab = pq.read_table(parquet_file)
sampled_rows = tab.take(random_indices)
end = datetime.datetime.now()
parquet_elapsed = (end - start).total_seconds()
print(f"Parquet elapsed: {parquet_elapsed}s")

# Function to read random rows from Lance file
def read_lance_file_random(lance_path, random_indices):
    for batch in (
        LanceFileReader(lance_path).take_rows(indices=random_indices).to_batches()
    ):
        pass

# Measure Lance read time
start = datetime.datetime.now()
read_lance_file_random(lance_file, random_indices)
end = datetime.datetime.now()
lance_elapsed = (end - start).total_seconds()
print(f"Lance elapsed: {lance_elapsed}s")

# Display file sizes
parquet_size = get_file_size(parquet_file)
lance_size = get_file_size(lance_file)
print(f"Parquet file size: {parquet_size}")
print(f"Lance file size: {lance_size}")

