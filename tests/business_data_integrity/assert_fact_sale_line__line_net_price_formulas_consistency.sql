{# 
  Verify that line_net_price calculated using two equivalent formulas matches 
  up to a small tolerance. 
 
  Formula 1: line_net_price = line_gross_price * (1 - discount_percentage / 100.0) * (1 + tax_percentage / 100.0)
  Formula 2: line_net_price = (line_gross_price - line_discount_value) * (1 + tax_percentage / 100)

  Since there may be small rounding differences, we compute the MAXIMUM discrepancy 
  in the CURRENT dataset and allow a slightly higher tolerance.

  So this test is intended for NEW entries.

  This is te query I ran to get the maximum discrepancy:

    WITH line_discrepancy AS (
    SELECT 
        order_id, part_id, supplier_id, line_number,
        ABS(
            line_gross_price * (1 - discount_percentage / 100.0) * (1 + tax_percentage / 100.0)
            - (line_gross_price - line_discount_value) * (1 + tax_percentage / 100.0)
        ) AS diff
    FROM trans_tpch_sf1__sale_info
)
SELECT MAX(diff) AS max_diff
FROM line_discrepancy;

    max_diff = 0.0054

#}

{% set tolerance_multiplier = 1.1 %}

{# Set tolerance slightly above max_diff #}
{% set tolerance = 0.0054 * tolerance_multiplier %}

{# Actual test: select any lines where the discrepancy exceeds the allowed tolerance #}
WITH line_discrepancy AS (
    SELECT sale_line_id,
           ABS(
               line_gross_price * (1 - discount_percentage / 100.0) * (1 + tax_percentage / 100.0)
               - (line_gross_price - line_discount_amount) * (1 + tax_percentage / 100.0)
           ) AS diff
    FROM {{ ref('fact_sale_line') }}
)
SELECT *
FROM line_discrepancy
WHERE diff > {{ tolerance }}