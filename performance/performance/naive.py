import json
from collections import defaultdict


def process_naive(path: str):

    accounts = defaultdict(int)

    with open(path, "r") as f:
        for line in f:
            row = json.loads(line)
            accounts[row["account_id"]] += int(row["amount"])

    return accounts
