WITH
trans_orders_priority AS(
    SELECT *
    FROM {{ ref('trans_tpch_sf1__order_priority') }}
)
SELECT *
FROM trans_orders_priority
ORDER BY priority_id ASC