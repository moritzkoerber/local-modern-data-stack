"""
This module defines and prepares a DbtProject object, making a dbt project accessible
and usable within Dagster.
"""

from pathlib import Path

from dagster_dbt import DbtProject

local_dagster = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..", "..").resolve(),
    packaged_project_dir=Path(__file__).joinpath("..", "..", "dbt-project").resolve(),
)
local_dagster.prepare_if_dev()
