import datetime
from time import perf_counter
import json
from pathlib import Path
import duckdb

from argparse import ArgumentParser

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

    parser = ArgumentParser()

    parser.add_argument("-e", "--engine")
    parser.add_argument("-f", "--format")

    args = parser.parse_args()

    engine_fn_map = {
        "naive": process_naive,
        "pandas": process_pandas,
        "pandas_naive": process_pandas_naive,
        "polars": process_polars,
        "duckdb": process_quack,
        "dask": process_dask,
    }

    if not args.engine:
        raise Exception("Please specify the engine type with --engine")

    engine = args.engine.strip().lower()

    if engine not in engine_fn_map:
        raise Exception(
            f"Unknown engine, supported engines are: {', '.join(engine_fn_map.keys())}"
        )

    fn = engine_fn_map[engine]

    start = perf_counter()
    results = fn(data_path)

    if engine == "naive":
        with open(root / "result_naive.json", "w") as f:
            f.write(json.dumps(results))
    elif engine == "pandas" or engine == "pandas_naive":
        results.to_json(str(root / "result_pandas.json"), index=False, orient="records")
    elif engine == "polars":
        results.write_json(str(root / "result_polars.json"), row_oriented=True)
    elif engine == "duckdb":
        file_format = args.format
        if not file_format or file_format.strip().lower() not in ("parquet", "json"):
            print(
                "File format not provided or not supported. Defaulting to  parquet; only parquet and json are supported"
            )
            file_format = "parquet"
        else:
            file_format = args.format.strip().lower()

        if file_format == "parquet":
            duckdb.sql(
                f"""copy results to '{str(root / "result_duckdb")}' (format parquet, per_thread_output true, overwrite_or_ignore true)"""
            )
        else:
            duckdb.sql(
                f"""copy results to '{str(root / "result_duckdb.json")}' (format  json)"""
            )
    else:
        results.to_json(str(root / "result_dask.json"), index=False, orient="records")

    end = perf_counter()

    print(f"Time: {end - start}")
