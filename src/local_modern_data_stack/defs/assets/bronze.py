import polars as pl
import requests
from dagster import asset
from deltalake.exceptions import TableNotFoundError

from .partitions import daily_partition


@asset(
    group_name="bronze",
    key_prefix=["bronze"],
    kinds={"python"},
    partitions_def=daily_partition,
)
def raw_xetra() -> None:
    response = requests.get(
        "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=MBG.DEX&outputsize=full&apikey=demo",
        timeout=180,
    )
    data = response.json()["Time Series (Daily)"]
    df = pl.DataFrame([{"trading_day": k, **v} for k, v in data.items()])
    print(f"Got {df.shape}")
    print(df.head())

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
