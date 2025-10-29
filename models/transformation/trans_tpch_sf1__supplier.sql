WITH
stage_supplier AS(
    SELECT *
    FROM {{ ref('stg_tpch_sf1__supplier') }}
),
final_supplier AS(
    SELECT supplier_id, supplier_code, country_id, encrypted_address, TRIM(phone) AS phone, COALESCE(account_balance, 0) AS account_balance
    FROM stage_supplier
)
SELECT *
FROM final_supplier