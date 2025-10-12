from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from ..resources import dbt_project


# A Dagster asset that, when materialized, will execute dbt build for the specified dbt
# project, effectively running all your dbt models and tests.
@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream().fetch_row_counts()
