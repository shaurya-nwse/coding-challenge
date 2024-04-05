import datetime
from time import perf_counter
import json
from pathlib import Path
import duckdb

from performance.process_file import (
    process_naive,
    process_pandas,
    process_pandas_naive,
    process_polars,
    process_quack,
    process_dask,
)


if __name__ == "__main__":
    root = Path(__file__).parent.parent
    data_path = str(root / "transactions.json")
    start = perf_counter()
    # results = process_pandas(data_path)
    # results = process_polars(data_path)
    results = process_dask(data_path)

    # duckdb.sql(f"""copy results to '{str(root / "result_duckdb.json")}'""")
    # duckdb.sql(
    #     f"""copy results to '{str(root / "result_duckdb_alt")}'
    #     (format parquet, per_thread_output true, overwrite_or_ignore true)"""
    # )
    end = perf_counter()
    print(f"Time: {end - start}")
    # results.write_json(str(root / "result_polars.json"), row_oriented=True)
    # results.write_json(str(root / "result_polars_mp.json"), row_oriented=True)
    results.to_json(str(root / "result_dask.json"), index=False, orient="records")

    # results.to_json(str(root / "result_pandas_v2.json"), index=False, orient="records")

    # with open(root / "result_naive.json", "w") as f:
    #     f.write(json.dumps(results))
