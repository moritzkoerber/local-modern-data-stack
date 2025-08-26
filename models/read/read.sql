SELECT
    *,
    current_date() AS run_date
FROM {{ source("covid19", "germany") }}
