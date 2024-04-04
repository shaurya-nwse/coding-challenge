import duckdb


def process_quack(path: str):

    return duckdb.sql(
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
