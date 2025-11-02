"""
This file centralizes the definitions of your Dagster assets, schedules, and resources,
making them available to Dagster tools like the UI and CLI. It is the entry point that
Dagster will load when deploying your code location.
"""

from pathlib import Path

from dagster import Definitions
from dagster_dbt import DbtCliResource
from dagster_duckdb import DuckDBResource

from .defs.assets.bronze import raw_xetra
from .defs.assets.dbt import incremental_dbt_models
from .defs.assets.presentation import xetra_closing_price_plot
from .defs.jobs import partitioned_asset_job
from .defs.resources import dbt_project
from .schedules import schedules

defs = Definitions(
    assets=[
        raw_xetra,
        incremental_dbt_models,
        xetra_closing_price_plot,
    ],
    schedules=schedules,
    jobs=[partitioned_asset_job],
    # In Dagster, Resources are the external services, tools, and storage backends
    # you need to do your job.
    resources={
        "dbt": DbtCliResource(project_dir=dbt_project),
        # required for the presentation layer assets
        "duckdb": DuckDBResource(database=str(Path() / "data/db.duckdb")),
    },
)
