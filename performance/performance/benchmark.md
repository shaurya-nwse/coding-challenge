# Benchmark Performance

- Naive python with json: 70 seconds
- Pandas v2 with JSON Reader: 81 seconds
- Pandas v2 Naive: 593 seconds
- Polars: 7-10 seconds
- DuckDB (write to JSON): 3-4 seconds

> **DuckDB (write to parquet per thread): 3-3.2 seconds - FASTEST**

- Dask Dataframe (unoptimized): 1457 seconds

# Instructions to run

1. Install poetry in your python environment

```shell
pip install poetry
```

2. Install the dependencies:

```shell
poetry install
```

3. Generate the dataset

```shell
python data_generator.py
```

Make sure its in the root of the project i.e. where the `requirements.txt` is.

4. Run the program:
   `engine` can be one of:

- polars
- duckdb
- pandas
- pandas_naive
- naive
- dask

if you choose `duckdb`, make sure to supply the format (either of `parquet` or `json`)

```shell
# for json output using the duckdb engine
python performance/main.py --engine duckdb --format json

# using polars
python performance/main.py --engine polars
```

5. Inspect the output as `result_<engine>.json` where `<engine>` would be the engine you chose

In case of any questions, reach out to `rawatshaurya1994@gmail.com`.
