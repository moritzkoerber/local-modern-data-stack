import logging

import polars as pl
import requests
from dagster import AssetExecutionContext, BackfillPolicy, asset
from deltalake.exceptions import TableNotFoundError

from .partitions import daily_partition

logger = logging.getLogger(__name__)


@asset(
    group_name="bronze",
    key_prefix=["bronze"],
    kinds={"python"},
    partitions_def=daily_partition,
    backfill_policy=BackfillPolicy.single_run(),
)
def raw_xetra(context: AssetExecutionContext) -> None:
    """
    Fetches raw XETRA stock data from Alpha Vantage API and stores it in Delta Lake format.
    The asset is partitioned by day and merges new data with existing records.

    Args:
        context: The execution context provided by Dagster, containing partition information and logging capabilities.
    """
    start, end = context.partition_time_window
    logger.debug("Processing XETRA data from %s to %s", start, end)

    response = requests.get(
        "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=MBG.DEX&outputsize=full&apikey=demo",
        timeout=180,
    )
    data = response.json()["Time Series (Daily)"]

    df = pl.DataFrame([{"trading_day": k, **v} for k, v in data.items()]).filter(
        pl.col("trading_day").is_between(
            pl.lit(start.strftime("%Y-%m-%d")),
            pl.lit(end.strftime("%Y-%m-%d")),
            closed="left",
        )
    )

    try:
        df.write_delta(
            "data/bronze/xetra",
            mode="merge",
            delta_merge_options={
                "predicate": "s.trading_day = t.trading_day",
                "source_alias": "s",
                "target_alias": "t",
            },
        ).when_not_matched_insert_all().execute()
    except TableNotFoundError:
        # If the table doesn't exist, create it first
        df.write_delta("data/bronze/xetra", mode="overwrite")
