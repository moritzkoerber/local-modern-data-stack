"""
This module defines schedules for automatically running Dagster jobs, such as
materializing dbt models at specified times.
"""

from dagster import (
    DefaultScheduleStatus,
    ScheduleDefinition,
    build_schedule_from_partitioned_job,
)

from .defs.jobs import full_load, partitioned_asset_job

asset_partitioned_schedule = build_schedule_from_partitioned_job(partitioned_asset_job)

daily_update_schedule = ScheduleDefinition(
    name="daily_update_xetra",
    job=full_load,
    cron_schedule="0 0 * * *",
    default_status=DefaultScheduleStatus.RUNNING,
)

schedules = [asset_partitioned_schedule, daily_update_schedule]
