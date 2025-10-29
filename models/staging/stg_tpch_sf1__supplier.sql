WITH
base_sup AS (
    SELECT *
    FROM {{ source("tpch_sf1", "supplier") }}
),
refined AS (
    SELECT
        s_suppkey AS supplier_id,
        SUBSTRING(s_name, 10, 10) AS supplier_code,
        s_address AS encrypted_address,
        s_nationkey AS country_id,
        s_phone AS phone,
        s_acctbal AS account_balance,
        s_comment AS comment
    FROM base_sup
)
SELECT *
FROM refined