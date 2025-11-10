from pathlib import Path
from typing import Final

from dagster_dbt import DbtCliResource, DbtProject
from dagster_duckdb import DuckDBResource

DBT_PROJECT_DIR: Final = Path() / "dbt"
DUCKDB_PATH: Final = Path() / "data/db.duckdb"

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)
dbt_resource = DbtCliResource(project_dir=dbt_project)

duckdb_resource = DuckDBResource(database=str(DUCKDB_PATH))
