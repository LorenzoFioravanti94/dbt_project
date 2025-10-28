WITH
stage_country AS(
    SELECT *
    FROM {{ ref('stg_tpch_sf1__country') }}
),
stage_country_region AS(
    SELECT *
    FROM {{ ref('stg_tpch_sf1__country_region') }}
),

trans_country AS(
    SELECT country_id, region_id, COALESCE(UPPER(TRIM(country_name)), 'UNKNOWN') AS country_name
    FROM stage_country
),
trans_country_region AS(
    SELECT region_id, COALESCE(UPPER(TRIM(region_name)), 'UNKNOWN') AS region_name
    FROM stage_country_region
),

final_country_per_region AS(
    SELECT t.country_id, t.country_name, t.region_id, tcr.region_name
    FROM trans_country AS t
    INNER JOIN trans_country_region AS tcr
    ON t.region_id = tcr.region_id
)
SELECT *
FROM final_country_per_region