import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Step 1: Read the TSV file into a pandas DataFrame
tsv_file = "/home/x/fulldocs.tsv"
df = pd.read_csv(tsv_file, sep="\t", header=None)

# Step 2: Convert each column to a separate Parquet file
for col_index in df.columns:
    # Convert the column to a PyArrow table
    column_data = pa.Table.from_pandas(df[[col_index]])
    # Define the output Parquet file name
    output_file = f"column_{col_index}.parquet"
    # Write the column to a Parquet file
    pq.write_table(column_data, output_file)
    print(f"Column {col_index} written to {output_file}")

