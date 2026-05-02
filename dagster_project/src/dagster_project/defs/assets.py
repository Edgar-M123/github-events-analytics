import dagster as dg
from dagster_gcp import BigQueryResource
import datetime as dt
from google.cloud import bigquery as bq
import requests
import io
import gzip

@dg.asset
def gh_events_json_gz(bigquery: BigQueryResource):

    dates = [(dt.datetime(year=2026, month=1, day=1) + dt.timedelta(hours=x)).strftime('%Y-%m-%d-%-H') for x in range(3)]
    table_name = "raw.github_events"


    json_schema = [
        bq.SchemaField("id", "STRING"),
        bq.SchemaField("type", "STRING"),
        bq.SchemaField("actor", "STRUCT", fields=[
            bq.SchemaField("id", "STRING"),
            bq.SchemaField("login", "STRING"),
            bq.SchemaField("display_login", "STRING"),
            bq.SchemaField("gravatar_id", "STRING"),
            bq.SchemaField("url", "STRING"),
            bq.SchemaField("avatar_url", "STRING"),
        ]),
        bq.SchemaField("repo", "STRUCT", fields=[
            bq.SchemaField("id", "STRING"),
            bq.SchemaField("name", "STRING"),
            bq.SchemaField("url", "STRING"),
        ]),
        bq.SchemaField("payload", "JSON"),
        bq.SchemaField("public", "BOOL"),
        bq.SchemaField("created_at", "TIMESTAMP"),
        bq.SchemaField("org", "STRUCT", fields=[
            bq.SchemaField("id", "STRING"),
            bq.SchemaField("login", "STRING"),
            bq.SchemaField("url", "STRING"),
            bq.SchemaField("gravatar_id", "STRING"),
            bq.SchemaField("avatar_url", "STRING"),
        ]),
    ]


    with bigquery.get_client() as client:
        table_ref = client.dataset("raw").table("github_events")

        for i, date in enumerate(dates):
            print(f"Reading data for {date}")
            url = f"https://data.gharchive.org/{date}.json.gz"
            res = requests.get(url)
            gzip_data = res.content

            json_data = gzip.decompress(gzip_data)

            if i == 0:
                job_config = bq.LoadJobConfig(
                    source_format=bq.SourceFormat.NEWLINE_DELIMITED_JSON,
                    schema=json_schema,
                    autodetect=True,
                    write_disposition=bq.WriteDisposition.WRITE_TRUNCATE,
                    ignore_unknown_values=True
                )

                job = client.load_table_from_file(
                    file_obj=io.BytesIO(json_data),
                    destination=table_ref,
                    job_config=job_config
                )
                
                job.result()


            else:
                job_config = bq.LoadJobConfig(
                    source_format=bq.SourceFormat.NEWLINE_DELIMITED_JSON,
                    schema=json_schema,
                    write_disposition=bq.WriteDisposition.WRITE_APPEND,
                    ignore_unknown_values=True
                )

                job = client.load_table_from_file(
                    file_obj=io.BytesIO(json_data),
                    destination=table_ref,
                    job_config=job_config
                )

                job.result()


@dg.asset_check(asset='gh_events_json_gz')
def check_events_json_loaded(bigquery: BigQueryResource) -> dg.AssetCheckResult:

    table_name = "raw.github_events"

    with bigquery.get_client() as client:
        row_count = client.query(
            f"""
            select count(*) from {table_name}
            """
        ).result()

        if row_count is None or row_count == 0:
            return dg.AssetCheckResult(
                passed = False, metadata={"message": "No data in this table."}
            )

        return dg.AssetCheckResult(
            passed = True, metadata={"message": "Data exists in this table."}
        )
