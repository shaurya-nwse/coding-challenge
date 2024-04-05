from __future__ import annotations

import json
from collections import defaultdict
import pandas as pd
import polars as pl
import duckdb
import dask.dataframe as dd


def process_pandas(path: str):

    shards = []

    frames = pd.read_json(path, lines=True, chunksize=1000)

    for frame in frames:
        df = (
            frame.drop_duplicates(subset=["transaction_id"])
            .groupby("account_id")
            .agg({"amount": "sum"})
            .reset_index()
        )
        shards.append(df)

    df = pd.concat(shards)

    return df.groupby("account_id").agg({"amount": "sum"}).reset_index()


def process_pandas_naive(path: str):

    return (
        pd.read_json(path, lines=True)
        .drop_duplicates(subset=["transaction_id"])
        .groupby("account_id")
        .agg({"amount": "sum"})
        .drop_duplicates()
        .reset_index()
    )


def process_polars(path: str):
    df = pl.scan_ndjson(path)

    return (
        df.lazy()
        .unique(subset=["transaction_id"])
        .group_by("account_id")
        .agg(pl.col("amount").sum())
        .collect()
    )


def process_quack(path: str):

    result = duckdb.sql(
        f"""
        with base as (
            select * from read_json('{path}', format='newline_delimited', ignore_errors=true)
        ), interim as (
        select
            *,
            row_number() over (partition by transaction_id) as rn
        from
            base
        )
        select
            account_id,
            sum(amount) as amount
        from
            interim
        where rn = 1
        group by 1
    """
    )

    return result


def process_naive(path: str):

    accounts = defaultdict(int)
    transactions = set()

    with open(path, "r") as f:
        for line in f:
            row = json.loads(line)

            if row["transaction_id"] in transactions:
                continue

            accounts[row["account_id"]] += int(row["amount"])
            transactions.add(row["transaction_id"])

    return accounts


def process_dask(path: str):

    import dask
    import multiprocessing as mp

    dask.config.set(scheduler="processes")
    dask.config.set(num_workers=mp.cpu_count())

    ddf = dd.read_json(path)

    return (
        ddf.drop_duplicates(subset=["transaction_id"])
        .groupby("account_id")
        .agg({"amount": "sum"})
        .compute()
    )
