from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from ..project import local_dagster


# A Dagster asset that, when materialized, will execute dbt build for the specified dbt
# project, effectively running all your dbt models and tests.
@dbt_assets(manifest=local_dagster.manifest_path)
def dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
