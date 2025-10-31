WITH
trans_sale_info AS(
    SELECT *
    FROM {{ ref('trans_tpch_sf1__sale_info') }}
),
date_enemble AS(
    SELECT order_date, ship_date, commit_date, receipt_date
    FROM trans_sale_info
),
one_column_date_ensemble AS(
    SELECT order_date AS date_value
    FROM date_enemble
    UNION
    SELECT ship_date
    FROM date_enemble
    UNION
    SELECT commit_date
    FROM date_enemble
    UNION
    SELECT receipt_date
    FROM date_enemble
),
one_column_distinct_date_ensemble AS(
    SELECT DISTINCT date_value
    FROM one_column_date_ensemble
),
dim_date AS(
    SELECT {{ dbt_utils.generate_surrogate_key(['date_value']) }} AS date_id, date_value
    FROM one_column_distinct_date_ensemble
    WHERE date_value IS NOT NULL
)
SELECT *
FROM dim_date
ORDER BY date_value
