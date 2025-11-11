from dagster import Definitions

from .defs.assets.bronze import raw_xetra
from .defs.assets.dbt import incremental_dbt_models
from .defs.assets.presentation import xetra_closing_price_plot
from .defs.jobs import partitioned_asset_job
from .defs.resources import dbt_resource, duckdb_resource
from .schedules import schedules

defs = Definitions(
    assets=[
        raw_xetra,
        incremental_dbt_models,
        xetra_closing_price_plot,
    ],
    schedules=schedules,
    jobs=[partitioned_asset_job],
    resources={"dbt": dbt_resource, "duckdb": duckdb_resource},
)
