WITH
stage_orders AS(
    SELECT *
    FROM {{ ref('stg_tpch_sf1__orders') }}
)
SELECT DISTINCT SPLIT_PART(order_priority, '-', 1)::INTEGER AS priority_id,
    UPPER(TRIM(SPLIT_PART(order_priority, '-', 2))) AS description
FROM stage_orders
ORDER BY priority_id ASC