from lance.file import LanceFileReader, LanceFileWriter
import pyarrow.parquet as pq

data = pq.read_table("/home/x/internship_summary/column_0.parquet")
with LanceFileWriter("/home/x/internship_summary/column_0.lance", version="2.1") as writer:
  writer.write_batch(data)
