from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from .project import duckdb_project


@dbt_assets(manifest=duckdb_project.manifest_path)
def duckdb_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
