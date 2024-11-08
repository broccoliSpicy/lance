import pyarrow as pa
import pyarrow.parquet as pq
import datetime
import os
from lance.file import LanceFileReader, LanceFileWriter
import pandas as pd

# Directory paths for input and output files
parquet_folder = "/home/x/tpch/tpch_sf10/"
lance_folder = "/home/x/tpch/tpch_sf10/"

# Ensure output folders exist
os.makedirs(parquet_folder, exist_ok=True)
os.makedirs(lance_folder, exist_ok=True)

# List of TPC-H table names
# tables = ["partsupp"]
tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]

# Function to format throughput
def format_throughput(value):
    return f"{value:.2f} GiB/s"

# Function to flush file cache
def flush_file_cache():
    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches')

# Batch size for reading
batch_size = 32 * 1024

# Benchmark each TPC-H table
for table in tables:
    print(f"Processing table: {table}")

    # File paths
    parquet_file_path = os.path.join(parquet_folder, f"{table}.parquet")
    lance_file_path = os.path.join(lance_folder, f"{table}.lance")

    # Read the table from Parquet
    table_parquet = pq.read_table(parquet_file_path)
    print(f"{table} rows: {table_parquet.num_rows}")

    # Write to Parquet and benchmark write time
    start = datetime.datetime.now()
    pq.write_table(table_parquet, parquet_file_path)
    end = datetime.datetime.now()
    elapsed_parquet_write = (end - start).total_seconds()
    print(f"Parquet write time for {table}: {elapsed_parquet_write:.2f}s")

    # Write to Lance and benchmark write time
    start = datetime.datetime.now()
    with LanceFileWriter(lance_file_path, version = "2.1") as writer:
        writer.write_batch(table_parquet)
    end = datetime.datetime.now()
    elapsed_lance_write = (end - start).total_seconds()
    print(f"Lance write time for {table}: {elapsed_lance_write:.2f}s")

    # Flush file cache
    flush_file_cache()

    # Benchmark read time for Parquet file
    start = datetime.datetime.now()
    parquet_file = pq.ParquetFile(parquet_file_path)
    batches = parquet_file.iter_batches(batch_size=batch_size)
    tab_parquet = pa.Table.from_batches(batches)
    end = datetime.datetime.now()
    elapsed_parquet_read = (end - start).total_seconds()
    print(f"Parquet read time for {table}: {elapsed_parquet_read:.2f}s")

    # Flush file cache again before reading Lance file
    flush_file_cache()

    # Benchmark read time for Lance file
    start = datetime.datetime.now()
    tab_lance = LanceFileReader(lance_file_path).read_all(batch_size=batch_size).to_table()
    end = datetime.datetime.now()
    elapsed_lance_read = (end - start).total_seconds()
    print(f"Lance read time for {table}: {elapsed_lance_read:.2f}s")

    # Compute total memory size
    parquet_memory_size = tab_parquet.get_total_buffer_size()
    lance_memory_size = tab_lance.get_total_buffer_size()

    # Convert memory size to GiB
    parquet_memory_size_gib = parquet_memory_size / (1024 * 1024 * 1024)
    lance_memory_size_gib = lance_memory_size / (1024 * 1024 * 1024)

    # Compute read throughput in GiB/sec
    throughput_parquet_gib = parquet_memory_size_gib / elapsed_parquet_read
    throughput_lance_gib = lance_memory_size_gib / elapsed_lance_read

    # Format throughput values
    formatted_throughput_parquet = format_throughput(throughput_parquet_gib)
    formatted_throughput_lance = format_throughput(throughput_lance_gib)

    # Output results
    print(f"Parquet read throughput for {table}: {formatted_throughput_parquet}")
    print(f"Lance read throughput for {table}: {formatted_throughput_lance}")

    # Check file sizes
    lance_file_size = os.path.getsize(lance_file_path)
    lance_file_size_mib = lance_file_size // 1048576
    parquet_file_size = os.path.getsize(parquet_file_path)
    parquet_file_size_mib = parquet_file_size // 1048576

    print(f"Parquet file size for {table}: {parquet_file_size} bytes ({parquet_file_size_mib:,} MiB)")
    print(f"Lance file size for {table}: {lance_file_size} bytes ({lance_file_size_mib:,} MiB)")

    # Verify data consistency between Parquet and Lance
    assert tab_parquet == tab_lance, f"Data mismatch in table {table}"

    print(f"Completed processing for table: {table}\n")

print("All tables processed successfully.")
