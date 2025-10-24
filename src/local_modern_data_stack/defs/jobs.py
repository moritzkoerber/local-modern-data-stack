from dagster import define_asset_job

from .assets.bronze import raw_xetra
from .assets.dbt import dbt_full_models, incremental_dbt_models
from .assets.presentation import xetra_closing_price_plot

full_load = define_asset_job(
    name="full_pipeline_job",
    selection=[raw_xetra, dbt_full_models, xetra_closing_price_plot],
)

partitioned_asset_job = define_asset_job(
    "partitioned_job",
    selection=[raw_xetra, incremental_dbt_models, xetra_closing_price_plot],
)
