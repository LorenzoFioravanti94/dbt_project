WITH
trans_customer AS(
    SELECT *
    FROM {{ ref('trans_tpch_sf1__customer') }}
),
dim_customer AS( -- just swapped customer_code and country_id and renamed country_id
    SELECT customer_id, country_id AS geography_id, customer_code, account_balance, market_segment, encrypted_address, phone, high_value_customer
    FROM trans_customer
)
SELECT *
FROM dim_customer
ORDER BY customer_id ASC