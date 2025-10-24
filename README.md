# Local modern data stack

A simple project that demos duckdb and delta lake leveraged by dbt and orchestrated by dagster. The stack queries data from an API and does an incremental load into a medaillon architecture. For just dbt, run `$ uvx --with dbt-duckdb --from dbt-core dbt run`.

`$ uv run dg dev`
