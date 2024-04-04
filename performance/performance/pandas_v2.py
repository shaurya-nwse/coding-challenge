import pandas as pd


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
        .groupby("account_id")
        .agg({"amount": "sum"})
        .drop_duplicates()
        .reset_index()
    )
