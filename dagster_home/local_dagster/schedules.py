"""
This module defines schedules for automatically running Dagster jobs, such as
materializing dbt models at specified times.
"""

from dagster import AssetSelection, ScheduleDefinition
from dagster_dbt import build_schedule_from_dbt_selection

from .assets.dbt import dbt_assets

dbt_schedule = build_schedule_from_dbt_selection(
    [dbt_assets],
    job_name="materialize_dbt_models",
    cron_schedule="0 11 * * *",
    dbt_select="fqn:*",
)


daily_update_schedule = ScheduleDefinition(
    name="daily_update_covid19_data_rki",
    target=AssetSelection.assets(["bronze", "covid19_data_rki"]),
    cron_schedule="0 0 * * *",
)

schedules = [dbt_schedule, daily_update_schedule]
