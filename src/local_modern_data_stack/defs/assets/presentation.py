from pathlib import Path

import plotly.express as px
from dagster import (
    AssetExecutionContext,
    MetadataValue,
    asset,
)
from dagster_dbt import get_asset_key_for_model
from dagster_duckdb import DuckDBResource

from .dbt import incremental_dbt_models


@asset(
    kinds={"python"},
    group_name="presentation",
    key_prefix=["presentation"],
    deps=[get_asset_key_for_model([incremental_dbt_models], "gold_xetra")],
)
def xetra_closing_price_plot(
    context: AssetExecutionContext, duckdb: DuckDBResource
) -> None:
    """
    Generates a line plot of Xetra closing prices over time using Plotly and saves it as an HTML file.

    Args:
        context: The execution context provided by Dagster.
        duckdb: The DuckDB resource for database interactions.
    """
    with duckdb.get_connection() as conn:
        cases = conn.sql("select * from main.gold_xetra").pl()

        fig = px.line(
            cases, x="date", y="closing_price", title="Xetra Closing Prices Over Time"
        )
        fig.update_layout(bargap=0.2)
        save_chart_path = Path() / "closing_price_chart.html"
        fig.write_html(save_chart_path)

        context.add_output_metadata(
            {"plot_url": MetadataValue.url(f"file://{save_chart_path}")}
        )
