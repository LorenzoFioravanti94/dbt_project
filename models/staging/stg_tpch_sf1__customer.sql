WITH
base_customer AS (
    SELECT *
    FROM {{ source("tpch_sf1", "customer") }}
),
refined AS (
    SELECT
        c_custkey AS customer_id,
        SUBSTRING(c_name, 10, 10) AS name,
        c_address AS encrypted_address,
        c_nationkey AS nationkey,
        c_phone AS phone,
        c_acctbal AS account_balance,
        c_mktsegment AS market_segment,
        c_comment AS comment
    FROM base_customer
)
SELECT *
FROM refined