import polars as pl
import requests
from dagster import asset
from deltalake.exceptions import TableNotFoundError


@asset(
    group_name="bronze",
    key_prefix=["bronze"],
    kinds={"python"},
)
def covid19_data_rki() -> None:
    response = requests.get("https://api.corona-zahlen.org/germany", timeout=180)
    df = pl.json_normalize(response.json())
    print(f"got {df.head()}")
    try:
        df.write_delta(
            "data/bronze/germany",
            mode="merge",
            delta_merge_options={
                "predicate": "s.`meta.lastUpdate` = t.`meta.lastUpdate`",
                "source_alias": "s",
                "target_alias": "t",
            },
        ).when_not_matched_insert_all().execute()
    except TableNotFoundError:
        # If the table doesn't exist, create it first
        df.write_delta("data/bronze/germany", mode="overwrite")
