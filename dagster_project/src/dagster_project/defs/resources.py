import dagster as dg
from dagster_duckdb import DuckDBResource
from dagster_gcp import BigQueryResource

database_resource = DuckDBResource(database="../databases/analytics.duckdb")

bigquery_resource = BigQueryResource(
    project="analytics-engineering-494923",
    location="US"
)


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(resources={
        "duckdb": database_resource,
        "bigquery": bigquery_resource
    })
