{# 
  Verify that the sum of line_net_price (calculated) per order is consistent with order_net_price (given from database). 
  Since there may be small rounding differences, I computed the MAXIMUM discrepancy in the CURRENT dataset 
  and I decided to allow a slightly larger tolerance for this test.

  So this test is intended for NEW entries.

  This is te query I ran to get the maximum discrepancy:

    WITH order_sums AS (
        SELECT order_id, order_net_price, SUM(line_net_price) AS sum_line_net_price, ABS(SUM(line_net_price) - order_net_price) AS diff
        FROM trans_tpch_sf1__sale_info
        GROUP BY order_id, order_net_price
    )
    SELECT MAX(diff) AS max_diff
    FROM order_sums;

    max_diff = 0.13

#}

{% set tolerance_multiplier = 1.1 %}

{# Set tolerance slightly above max_diff #}
{% set tolerance = 0.13 * tolerance_multiplier %}

{# Actual test: select any orders that exceed the allowed tolerance #}
WITH order_discrepancy AS (
    SELECT order_id, order_net_price, SUM(line_net_price) AS sum_line_net_price, ABS(SUM(line_net_price) - order_net_price) AS diff
    FROM {{ ref('fact_sale_line') }}
    GROUP BY order_id, order_net_price
)
SELECT *
FROM order_discrepancy
WHERE diff > {{ tolerance }}