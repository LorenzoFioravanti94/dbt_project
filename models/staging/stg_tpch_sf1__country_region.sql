WITH
base_cr AS (
    SELECT *
    FROM {{ source("tpch_sf1", "region") }}
),
refined AS (
    SELECT
        r_regionkey AS region_id,
        r_name AS region_name,
        r_comment AS comment
    FROM base_cr
)
SELECT *
FROM refined