"""This module defines schedules for automatically running Dagster jobs, such as materializing dbt models at specified times."""

from dagster import AssetSelection, ScheduleDefinition
from dagster_dbt import build_schedule_from_dbt_selection

from .assets import local_dagster_assets

dbt_schedule = build_schedule_from_dbt_selection(
    [local_dagster_assets],
    job_name="materialize_dbt_models",
    cron_schedule="0 11 * * *",
    dbt_select="fqn:*",
)


daily_update_schedule = ScheduleDefinition(
    name="daily_update_covid19_data_rki",
    target=AssetSelection.keys("covid19_data_rki").upstream(),
    cron_schedule="0 0 * * *",
)

schedules = [dbt_schedule, daily_update_schedule]
