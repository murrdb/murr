import pyarrow as pa
import numpy as np
import pyarrow.parquet as pq

keys = pa.array([f"key{i}" for i in range(1, 10000)])
col = pa.array(np.arange(1, 10000))
table = pa.Table.from_arrays([keys, col], names=["key", "value"])
pq.write_table(table, "example.parquet")
