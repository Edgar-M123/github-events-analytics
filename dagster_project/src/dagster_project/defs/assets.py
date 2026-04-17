import dagster as dg
import pandas as pd
from dagster_duckdb import DuckDBResource
import datetime as dt

@dg.asset
def gh_events_json_gz(duckdb: DuckDBResource):

    dates = [(dt.datetime(year=2026, month=1, day=1) + dt.timedelta(hours=x)).strftime('%Y-%m-%d-%-H') for x in range(3)]
    table_name = "raw.github_events"

    with duckdb.get_connection() as conn:
        for i, date in enumerate(dates):
            print(f"Reading data for {date}")
            url = f"https://data.gharchive.org/{date}.json.gz"
            
            if i == 0:
                conn.execute(
                    f"""
                    create or replace table {table_name} as (
                        select *, current_date as loaded_on from read_json_auto('{url}', union_by_name=true, map_inference_threshold=0)
                    );
                    """
                )
            else:
                conn.execute(
                    f"""
                    insert into {table_name} 
                    select *, current_date as loaded_on from read_json_auto('{url}', union_by_name=true, map_inference_threshold=0);
                    """
                )


@dg.asset_check(asset='gh_events_json_gz')
def check_events_json_loaded(duckdb: DuckDBResource) -> dg.AssetCheckResult:

    table_name = "raw.github_events"

    with duckdb.get_connection() as conn:
        row_count = conn.execute(
            f"""
            select count(*) from {table_name}
            """
        ).fetchone()

        if row_count is None or row_count == 0:
            return dg.AssetCheckResult(
                passed = False, metadata={"message": "No data in this table."}
            )

        return dg.AssetCheckResult(
            passed = True, metadata={"message": "Data exists in this table."}
        )


@dg.asset
def customers(duckdb: DuckDBResource):
    url = "https://raw.githubusercontent.com/dbt-labs/jaffle-shop-classic/refs/heads/main/seeds/raw_customers.csv"
    table_name = "raw.customers"

    with duckdb.get_connection() as conn:
        conn.execute(
            f"""
            create or replace table {table_name} as (
                select * from read_csv_auto('{url}')
            );
            """
        )

@dg.asset(
    deps=["customers"]
)
def customers_with_rank(duckdb: DuckDBResource):

    table_name = "int.customers_ranked"

    with duckdb.get_connection() as conn:
        conn.execute(
            f"""
            create or replace table {table_name} as (
                select
                    id,
                    first_name,
                    last_name,
                    rank() over (order by last_name) as last_name_order
                from raw.customers
            );
            """
        )

@dg.asset_check(asset="customers_with_rank")
def customers_with_rank_row_check(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    table_name = "int.customers_ranked"

    with duckdb.get_connection() as conn:
        row_count = conn.execute(
            f"""
            select count(*) from {table_name}
            """
        ).fetchone()

        if row_count is None or row_count == 0:
            return dg.AssetCheckResult(
                passed = False, metadata={"message": "No data in this table."}
            )

        return dg.AssetCheckResult(
            passed = True, metadata={"message": "Data exists in this table."}
        )
