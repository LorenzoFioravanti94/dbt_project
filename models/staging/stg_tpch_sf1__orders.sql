WITH
base_order AS (
    SELECT *
    FROM {{ source("tpch_sf1", "orders") }}
),
refined AS (
    SELECT
        o_orderkey AS order_id,
        o_custkey AS customer_id,
        o_orderstatus AS status,
        o_totalprice AS price,
        o_orderdate AS order_date,
        o_orderpriority AS order_priority,
        SUBSTRING(o_clerk, 7, 10) AS clerk,
        o_shippriority AS ship_priority,
        o_comment AS comment
    FROM base_order
)
SELECT *
FROM refined