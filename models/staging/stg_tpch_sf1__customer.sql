WITH
base_customer AS (
    SELECT *
    FROM {{ source("tpch_sf1", "customer") }}
),
refined AS (
    SELECT
        c_custkey AS customer_id,
        SUBSTRING(c_name, 10, 10) AS customer_code,
        c_address AS encrypted_address,
        c_nationkey AS country_code,
        c_phone AS phone,
        c_acctbal AS account_balance,
        c_mktsegment AS market_segment,
        c_comment AS comment
    FROM base_customer
)
SELECT *
FROM refined