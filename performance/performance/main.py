from time import perf_counter
import json
from pathlib import Path
import duckdb
from performance.naive import process_naive
from performance.pandas_v2 import process_pandas, process_pandas_naive
from performance.polars_rs import process_polars
from performance.duck import process_quack


if __name__ == "__main__":
    root = Path(__file__).parent.parent
    data_path = str(root / "transactions.json")
    start = perf_counter()
    # results = process_pandas(data_path)
    # results = process_polars(data_path)
    results = process_quack(data_path)

    # duckdb.sql(f"""copy results to '{str(root / "result_duckdb.json")}'""")
    duckdb.sql(
        f"""copy results to '{str(root / "result_duckdb_alt")}' 
        (format parquet, per_thread_output true, overwrite_or_ignore true)"""
    )
    end = perf_counter()
    print(f"Time: {end - start}")
    # results.write_json(str(root / "result_polars.json"), row_oriented=True)

    # results.to_json(str(root / "result_pandas_v2.json"), index=False, orient="records")

    # with open(root / "result_naive.json", "w") as f:
    #     f.write(json.dumps(results))
