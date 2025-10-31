WITH
trans_part AS (
    SELECT *
    FROM {{ ref('trans_tpch_sf1__part') }}
)
SELECT *
FROM trans_part
ORDER BY part_id ASC