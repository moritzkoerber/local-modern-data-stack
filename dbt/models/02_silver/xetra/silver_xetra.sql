{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'trading_day',
    on_schema_change = 'fail'
) }}

SELECT
    CAST(trading_day AS TIMESTAMP) AS trading_day,
    CAST("1. open" AS FLOAT) AS opening_price,
    CAST("2. high" AS FLOAT) AS highest_price,
    CAST("3. low" AS FLOAT) AS lowest_price,
    CAST("4. close" AS FLOAT) AS closing_price,
    CAST("5. volume" AS FLOAT) AS trade_volume
FROM {{ source("stocks", "xetra") }}
{% if is_incremental() %}
    WHERE trading_day >= '{{ var('start_date') }}' AND trading_day < '{{ var('end_date') }}'
{% endif %}
