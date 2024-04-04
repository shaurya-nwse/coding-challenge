Benchmark Performance
===

- Naive python with json: 70 seconds
- Pandas v2 with JSON Reader: 81 seconds
- Pandas v2 Naive: 593 seconds
- Polars: 7-10 seconds
- DuckDB (write to JSON): 3-4 seconds
- DuckDB (write to parquet per thread): 3-3.2 seconds
