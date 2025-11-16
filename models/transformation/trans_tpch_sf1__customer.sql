WITH
stage_customer AS(
    SELECT *
    FROM {{ ref('stg_tpch_sf1__customer') }}
),
trans_customer AS(
    SELECT customer_id, TRIM(customer_code) AS customer_code, country_id, COALESCE(account_balance, 0) AS account_balance,
        COALESCE(UPPER(TRIM(market_segment)), 'UNKNOWN') AS market_segment, encrypted_address, phone
    FROM stage_customer
),
avg_balance AS(
    SELECT AVG(account_balance) AS avg_b
    FROM trans_customer
),
final_customer AS(
    SELECT t.*,
    CASE
        WHEN t.account_balance > a.avg_b THEN TRUE
        ELSE FALSE
    END AS is_high_value_customer
    FROM trans_customer AS t
    CROSS JOIN avg_balance AS a
)
SELECT *
FROM final_customer