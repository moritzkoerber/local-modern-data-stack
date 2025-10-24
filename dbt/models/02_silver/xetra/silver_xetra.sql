{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'fail'
) }}

WITH src AS (
    SELECT
        CAST(trading_day AS TIMESTAMP) AS trading_day,
        "1. open" AS opening_price,
        "2. high" AS highest_price,
        "3. low" AS lowest_price,
        "4. close" AS closing_price,
        "5. volume" AS trade_volume
    FROM {{ source("stocks", "xetra") }}
    {% if is_incremental() %}
        WHERE
            CAST(trading_day AS TIMESTAMP)
            > (
                SELECT
                    COALESCE(
                        MAX(t.trading_day), TIMESTAMP '1900-01-01 00:00:00'
                    )
                FROM {{ this }} AS t
            )
    {% endif %}
)

SELECT * FROM src
