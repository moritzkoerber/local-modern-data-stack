# Local modern data stack

A simple project that demos duckdb and delta lake leveraged by dbt and orchestrated by Dagster. The stack queries Xetra data from an API and does an incremental load into a medallion architecture. Finally, a simply line chart is created via plotly. A schedule runs the whole pipeline once per day. Run the job to create a simple visualization.

## How to run

Simply run `$ uv run dg dev` to launch the dagster webserver.
