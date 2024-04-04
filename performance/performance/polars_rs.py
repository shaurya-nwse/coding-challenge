import polars as pl


def process_polars(path: str):
    df = pl.scan_ndjson(path)

    return (
        df.lazy()
        .unique(subset=["transaction_id"])
        .group_by("account_id")
        .agg(pl.col("amount").sum())
        .collect()
    )
