'''
from pathlib import Path

import pyarrow as pa
import pytest
import pandas as pd
from lance.file import LanceFileReader, LanceFileWriter
from lance.tracing import trace_to_chrome


trace_to_chrome(level="debug", file="/tmp/trace.json")
#NUM_ROWS = 10_000_000
NUM_ROWS = 1_7000
def test_scan_integer(tmp_path: Path):
    schema = pa.schema([pa.field("values", pa.int64(), True)])

    def gen_data():
        remaining = NUM_ROWS
        offset = 0
        #        let array = Int64Array::from(vec![1000; 16 * 1024 * 1024]);
        while remaining > 0:
            to_take = min(remaining, 10000)
            # values = pa.array(range(offset, offset + to_take))
            values = pa.array([1000] * to_take)
            batch = pa.table({"values": values}).to_batches()[0]
            yield batch
            remaining -= to_take
            offset += to_take

    with LanceFileWriter(
        str(tmp_path / "file.lance"), schema, version="2.1"
    ) as writer:
        for batch in gen_data():
            writer.write_batch(batch)

    def read_all():
        reader = LanceFileReader(str(tmp_path / "file.lance"))
        return reader.read_all(batch_size=16 * 1024).to_table()
    table = read_all()
    df = table.to_pandas()
    pd.set_option('display.max_rows', None)

    #result = benchmark.pedantic(read_all, rounds=1, iterations=1)

    # assert result.num_rows == NUM_ROWS

def test_scan_nullable_integer(tmp_path: Path, version="2.1"):
    schema = pa.schema([pa.field("values", pa.int64(), True)])

    def gen_data():
        remaining = 1024 * 1024
        offset = 0
        while remaining > 0:
            to_take = min(remaining, 10000)
            # Create an array with values alternating between None and 1000, with int64 type
            values = pa.array([None if i % 2 == 0 else 1000 for i in range(to_take)], type=pa.int64())
            batch = pa.table({"values": values}).to_batches()[0]
            yield batch
            remaining -= to_take
            offset += to_take

    with LanceFileWriter(
        str(tmp_path / "file.lance"), schema, version=version
    ) as writer:
        for batch in gen_data():
            writer.write_batch(batch)

    def read_all():
        reader = LanceFileReader(str(tmp_path / "file.lance"))
        return reader.read_all(batch_size=16 * 1024).to_table()

    table = read_all()
    df = table.to_pandas()
    pd.set_option('display.max_rows', None)
    print(df)
    print("Test succeeded!")

def test_scan_nested_integer(tmp_path: Path):
    def get_val(i: int):
        if i % 4 == 0:
            return None
        elif i % 4 == 1:
            return {"outer": None}
        elif i % 4 == 2:
            return {"outer": {"inner": None}}
        else:
            return {"outer": {"inner": i}}

    dtype = pa.struct(
        [pa.field("outer", pa.struct([pa.field("inner", pa.uint64(), True)]), True)]
    )
    schema = pa.schema(
        [
            pa.field(
                "values",
                dtype,
                True,
            )
        ]
    )

    def gen_data():
        remaining = 100
        offset = 0
        while remaining > 0:
            to_take = min(remaining, 10000)
            values = pa.array([get_val(i) for i in range(offset, offset + to_take)])
            batch = pa.table({"values": values}).to_batches()[0]
            yield batch
            remaining -= to_take
            offset += to_take

    with LanceFileWriter(str(tmp_path / "file.lance"), schema, version="2.1") as writer:
        for batch in gen_data():
            writer.write_batch(batch)

    def read_all():
        reader = LanceFileReader(str(tmp_path / "file.lance"))
        return reader.read_all(batch_size=16 * 1024).to_table()

    table = read_all()
    df = table.to_pandas()
    pd.set_option('display.max_rows', None)
    print(df)
    print("Test succeeded!")
    #result = benchmark.pedantic(read_all, rounds=1, iterations=1)

    # assert result.num_rows == NUM_ROWS

if __name__ == "__main__":
    test_scan_integer(Path("/home/admin/tmp"))
    #test_scan_nested_integer(Path("/home/admin/tmp"))
'''





import pyarrow as pa
import pyarrow.parquet as pq
import datetime
import os
import numpy as np
from lance.file import LanceFileReader, LanceFileWriter

def format_throughput(value):
    return f"{value:.2f} GiB/s"

parquet_file_path = "/home/admin/tmp/int64_column.parquet"
lance_file_path = "/home/admin/tmp/int64_column.lance"

# Generate 1024 * 1024 * 512 random values modulo 1024 using NumPy
values = np.random.randint(0, 1024, size=1024 * 1024 * 512, dtype=np.int64)
int64_array = pa.array(values, type=pa.int64())
table = pa.Table.from_arrays([int64_array], names=['int64_column'])

# Write the table to a Parquet file with Snappy compression
pq.write_table(table, parquet_file_path, compression='snappy')

# Write the table to a Lance file
with LanceFileWriter(lance_file_path, version="2.0") as writer:
    writer.write_batch(table)

# Flush file from kernel cache
os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches')

# Measure read time for Parquet file with batch size
batch_size = 32 * 1024
start = datetime.datetime.now()
parquet_file = pq.ParquetFile(parquet_file_path)
batches = parquet_file.iter_batches(batch_size=batch_size)
tab_parquet = pa.Table.from_batches(batches)
end = datetime.datetime.now()
elapsed_parquet = (end - start).total_seconds()

# Flush file from kernel cache again before reading Lance file
os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches')

# Measure read time for Lance file with batch size
start = datetime.datetime.now()
tab_lance = LanceFileReader(lance_file_path).read_all(batch_size=batch_size).to_table()
end = datetime.datetime.now()
elapsed_lance = (end - start).total_seconds()

num_rows = tab_parquet['int64_column'].length()

print(f"Number of rows: {num_rows}")
print(f"Parquet read time: {elapsed_parquet:.2f}s")
print(f"Lance read time: {elapsed_lance:.2f}s")

# Compute total memory size
parquet_memory_size = tab_parquet.get_total_buffer_size()
lance_memory_size = tab_lance.get_total_buffer_size()

# Convert memory size to GiB
parquet_memory_size_gib = parquet_memory_size / (1024 * 1024 * 1024)
lance_memory_size_gib = lance_memory_size / (1024 * 1024 * 1024)

# Compute read throughput in GiB/sec
throughput_parquet_gib = parquet_memory_size_gib / elapsed_parquet
throughput_lance_gib = lance_memory_size_gib / elapsed_lance

# Format throughput values
formatted_throughput_parquet_gib = format_throughput(throughput_parquet_gib)
formatted_throughput_lance_gib = format_throughput(throughput_lance_gib)

print(f"Parquet read throughput: {formatted_throughput_parquet_gib}")
print(f"Lance read throughput: {formatted_throughput_lance_gib}")

# Check file sizes
lance_file_size = os.path.getsize(lance_file_path)
lance_file_size_mib = lance_file_size // 1048576
parquet_file_size = os.path.getsize(parquet_file_path)
parquet_file_size_mib = parquet_file_size // 1048576

print(f"Parquet file size: {parquet_file_size} bytes ({parquet_file_size_mib:,} MiB)")
print(f"Lance file size: {lance_file_size} bytes ({lance_file_size_mib:,} MiB)")

# Assert that the tables are equal
assert tab_parquet == tab_lance