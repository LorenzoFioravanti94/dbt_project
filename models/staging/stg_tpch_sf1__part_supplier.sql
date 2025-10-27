WITH
base_ps AS (
    SELECT *
    FROM {{ source("tpch_sf1", "partsupp") }}
),
refined AS (
    SELECT
        ps_partkey AS part_id,
        ps_suppkey AS supplier_id,
        ps_availqty AS available_quantity,
        ps_supplycost AS supply_cost,
        ps_comment AS comment
    FROM base_ps
)
SELECT *
FROM refined