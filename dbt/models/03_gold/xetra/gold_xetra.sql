SELECT
    trading_day AS trading_date,
    closing_price
FROM {{ ref('silver_xetra') }}
