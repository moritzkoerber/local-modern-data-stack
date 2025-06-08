from dagster import AssetExecutionContext, asset
from dagster_dbt import DbtCliResource, dbt_assets
from dagster_duckdb import DuckDBResource

from .project import local_dagster


@asset(compute_kind="python")
def area1(context: AssetExecutionContext, duckdb: DuckDBResource) -> None:
    with duckdb.get_connection() as conn:
        table_name = "area1"
        schema = "local"
        conn.execute(f"create schema if not exists {schema}")

        conn.execute(
            f"create or replace table {schema}.{table_name} as select * from read_parquet('../data/area1_small.parquet')"
        )

        # Get row count for metadata
        result = conn.execute(f"select count(*) from {schema}.{table_name}").fetchone()
        # Log some metadata about the table we just wrote
        context.add_output_metadata({"num_rows": result[0]})


@dbt_assets(manifest=local_dagster.manifest_path)
def local_dagster_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
