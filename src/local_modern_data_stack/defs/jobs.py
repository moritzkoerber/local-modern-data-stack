from dagster import define_asset_job

from .assets.bronze import raw_xetra
from .assets.dbt import incremental_dbt_models
from .assets.presentation import xetra_closing_price_plot

partitioned_asset_job = define_asset_job(
    "partitioned_job",
    selection=[raw_xetra, incremental_dbt_models, xetra_closing_price_plot],
)
