from pathlib import Path

import plotly.express as px
from dagster import (
    AssetExecutionContext,
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
        closing_prices = conn.sql(
            "SELECT * FROM main.gold_xetra ORDER BY trading_date ASC"
        ).pl()

        fig = px.line(
            closing_prices,
            x="trading_date",
            y="closing_price",
            title="Xetra Closing Prices Over Time",
            labels={
                "trading_date": "Trading Date",
                "closing_price": "Closing Price (EUR)",
            },
        )

        save_chart_path = Path() / "presentation" / "closing_price_chart.html"
        fig.write_html(save_chart_path)
