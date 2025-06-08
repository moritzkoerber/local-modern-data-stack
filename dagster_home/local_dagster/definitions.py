from dagster import Definitions
from dagster_dbt import DbtCliResource
from dagster_duckdb import DuckDBResource

from .assets import area1, local_dagster_assets
from .project import local_dagster
from .schedules import schedules

defs = Definitions(
    assets=[local_dagster_assets, area1],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=local_dagster),
        "duckdb": DuckDBResource(database="../db.duckdb"),
    },
)
