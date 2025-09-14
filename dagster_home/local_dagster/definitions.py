"""
This file centralizes the definitions of your Dagster assets, schedules, and resources,
making them available to Dagster tools like the UI and CLI.
"""

from dagster import Definitions
from dagster_dbt import DbtCliResource
from dagster_duckdb import DuckDBResource

from .assets.bronze import covid19_data_rki
from .assets.dbt import dbt_assets
from .assets.presentation import cases_barchart
from .project import local_dagster
from .schedules import schedules

defs = Definitions(
    assets=[dbt_assets, covid19_data_rki, cases_barchart],
    schedules=schedules,
    # In Dagster, Resources are the external services, tools, and storage backends
    # you need to do your job. For the storage backend in this project, we'll use DuckDB
    resources={
        "dbt": DbtCliResource(project_dir=local_dagster),
        "duckdb": DuckDBResource(database="../db.duckdb"),
    },
)
