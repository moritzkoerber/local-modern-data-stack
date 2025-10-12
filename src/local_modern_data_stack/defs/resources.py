from pathlib import Path

import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject

dbt_project_directory = Path() / "dbt"
dbt_project = DbtProject(project_dir=dbt_project_directory)

dbt_resource = DbtCliResource(project_dir=dbt_project)


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "dbt": dbt_resource,
        }
    )
