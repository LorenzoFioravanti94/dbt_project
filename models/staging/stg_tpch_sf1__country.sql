WITH
base_country AS (
    SELECT *
    FROM {{ source("tpch_sf1", "nation") }}
),
refined AS (
    SELECT
        n_nationkey AS country_id,
        n_name AS country,
        n_regionkey AS region_id,
        n_comment AS comment
    FROM base_country
)
SELECT *
FROM refined