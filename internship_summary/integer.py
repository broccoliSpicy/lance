import os
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import time
from lance.file import LanceFileReader, LanceFileWriter

# Function to get file size in human-readable format
def get_file_size(file_path):
    size_bytes = os.path.getsize(file_path)
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} TB"

# Part 1: Generate the Parquet File
# Parameters
num_rows = 1_000_000_000
parquet_file = "/home/x/int64_data.parquet"
lance_file = "/home/x/int64_data.lance"

# Step 1: Generate random int32 data in [0, 1024 * 1024)
data = np.random.randint(low=0, high=1024 * 1024, size=num_rows, dtype=np.int64)

# Step 2: Convert to PyArrow Table
table = pa.Table.from_arrays([pa.array(data)], names=["column_int64"])

# Step 3: Write to Parquet
pq.write_table(table, parquet_file)
parquet_size = get_file_size(parquet_file)
print(f"Generated Parquet file: {parquet_file} with {num_rows} rows.")
print(f"Parquet file size: {parquet_size}")

# Part 1.1: Write to Lance File
with LanceFileWriter(lance_file, table.schema, version="2.1") as writer:
    for batch in table.to_batches():
        writer.write_batch(batch)

lance_size = get_file_size(lance_file)
print(f"Generated Lance file: {lance_file} with {num_rows} rows.")
print(f"Lance file size: {lance_size}")

# Part 2: Randomly Sample Rows and Benchmark Read Time for Parquet
# Parameters
num_rows_to_sample = 100

# Step 1: Get the total number of rows in the Parquet file
total_rows = num_rows

# Step 2: Randomly select 1,000 row indices
random_indices = np.random.choice(total_rows, size=num_rows_to_sample, replace=False)
random_indices.sort()

# Step 3: Read and sample the rows, benchmarking the process
start_time = time.time()

# Read the Parquet file and take the selected rows
table = pq.read_table(parquet_file)
sampled_rows = table.take(random_indices)

end_time = time.time()
print(f"Time taken to read and sample {num_rows_to_sample} rows from Parquet: {end_time - start_time:.4f} seconds")

# Part 3: Randomly Sample Rows and Benchmark Read Time for Lance
# Step 1: Benchmark the Lance file read
def read_lance_file_random(lance_path, random_indices):
    for batch in (
        LanceFileReader(lance_path).take_rows(indices=random_indices).to_batches()
    ):
        pass

start_time = time.time()

# Read the random rows from Lance file
read_lance_file_random(lance_file, random_indices)

end_time = time.time()
print(f"Time taken to read and sample {num_rows_to_sample} rows from Lance: {end_time - start_time:.4f} seconds")

