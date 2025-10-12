{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'fail'
) }}

WITH src AS (
    SELECT
        cases,
        deaths,
        recovered,
        weekincidence,
        casesper100k,
        casesperweek,
        deathsperweek,
        "delta.cases" AS delta_cases,
        "delta.deaths" AS delta_deaths,
        "delta.recovered" AS delta_recovered,
        "delta.weekIncidence" AS delta_weekincidence,
        "r.value" AS r_value,
        "r.rValue4Days.value" AS r_rvalue4days_value,
        "r.rValue7Days.value" AS r_rvalue7days_value,
        "hospitalization.cases7Days" AS hospitalization_cases7days,
        "hospitalization.incidence7Days" AS hospitalization_incidence7days,
        "meta.source" AS meta_source,
        "meta.contact" AS meta_contact,
        "meta.info" AS meta_info,
        CAST("r.rValue4Days.date" AS TIMESTAMP) AS r_rvalue4days_date,
        CAST("r.rValue7Days.date" AS TIMESTAMP) AS r_rvalue7days_date,
        CAST("r.lastUpdate" AS TIMESTAMP) AS r_lastupdate,
        CAST("hospitalization.date" AS TIMESTAMP) AS hospitalization_date,
        CAST("hospitalization.lastUpdate" AS TIMESTAMP)
            AS hospitalization_lastupdate,
        CAST("meta.lastUpdate" AS TIMESTAMP) AS meta_lastupdate,
        CAST("meta.lastCheckedForUpdate" AS TIMESTAMP)
            AS meta_lastcheckedforupdate,
        CURRENT_DATE() AS run_date
    FROM {{ source("covid19", "germany") }}
    {% if is_incremental() %}
        WHERE
            CAST(meta_lastupdate AS TIMESTAMP)
            > (
                SELECT
                    COALESCE(
                        MAX(t.meta_lastupdate), TIMESTAMP '1900-01-01 00:00:00'
                    )
                FROM {{ this }} AS t
            )
    {% endif %}
)

SELECT * FROM src
