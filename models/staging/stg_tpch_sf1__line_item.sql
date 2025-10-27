WITH
base_li AS (
    SELECT *
    FROM {{ source("tpch_sf1", "lineitem") }}
),
refined AS (
    SELECT
        l_orderkey AS order_id,
        l_partkey AS part_id,
        l_suppkey AS supplier_id,
        l_linenumber AS line_number,
        l_quantity::INTEGER AS quantity,
        l_extendedprice AS extended_price,
        (l_discount * 100)::INTEGER AS discount_percentage,
        (l_tax * 100)::INTEGER AS tax_percentage,
        l_returnflag AS return_flag,
        l_linestatus AS line_status,
        l_shipdate AS ship_date,
        l_commitdate AS commit_date,
        l_receiptdate AS receipt_date,
        l_shipinstruct AS ship_instructions,
        l_shipmode AS ship_mode,
        l_comment AS comment
    FROM base_li
)
SELECT *
FROM refined