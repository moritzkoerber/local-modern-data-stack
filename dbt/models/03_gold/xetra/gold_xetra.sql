{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'trading_date',
    on_schema_change = 'fail'
) }}

SELECT
    trading_day AS trading_date,
    closing_price
FROM {{ ref('silver_xetra') }}
{% if is_incremental() %}
    WHERE
        trading_day >= '{{ var('start_date') }}'
        AND trading_day < '{{ var('end_date') }}'
{% endif %}
