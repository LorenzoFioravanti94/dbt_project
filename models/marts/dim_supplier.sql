WITH
trans_supplier AS (
    SELECT *
    FROM {{ ref('trans_tpch_sf1__supplier') }}
)
SELECT supplier_id, country_id AS geography_id, supplier_code, account_balance, encrypted_address, phone
FROM trans_supplier
ORDER BY supplier_id ASC