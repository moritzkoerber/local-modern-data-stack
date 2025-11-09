import json
from typing import Iterator

from dagster import (
    AssetCheckEvaluation,
    AssetCheckResult,
    AssetExecutionContext,
    AssetMaterialization,
    AssetObservation,
    BackfillPolicy,
    Output,
)
from dagster_dbt import DbtCliResource, dbt_assets

from ..resources import dbt_project
from .partitions import daily_partition

INCREMENTAL_SELECTOR = "config.materialized:incremental"


@dbt_assets(
    manifest=dbt_project.manifest_path,
    select=INCREMENTAL_SELECTOR,
    partitions_def=daily_partition,
    backfill_policy=BackfillPolicy.single_run(),
)
def incremental_dbt_models(
    context: AssetExecutionContext, dbt: DbtCliResource
) -> Iterator[
    Output
    | AssetMaterialization
    | AssetObservation
    | AssetCheckResult
    | AssetCheckEvaluation
]:
    """Asset for incremental dbt models.

    Runs dbt models materialized as 'incremental' within the specified partition time
    window.

    Args:
        context: The execution context (partition information etc.).
        dbt: The dbt CLI resource to run dbt commands.

    Yields:
        Outputs, materializations, observations, and check results from the dbt run.
    """
    time_window = context.partition_time_window
    dbt_vars = {
        "start_date": time_window.start.strftime("%Y-%m-%d"),
        "end_date": time_window.end.strftime("%Y-%m-%d"),
    }

    yield from dbt.cli(
        ["build", "--vars", json.dumps(dbt_vars)], context=context
    ).stream()
