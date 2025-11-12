{# 
  Since the source database contains sales records from the '90s, 
  every NEW order entry must reference an order within that period.

  Date limits were computed with:

    SELECT MIN(order_date) AS min_ord_date, MAX(order_date) AS max_ord_date
    FROM stg_tpch_sf1__orders;

      min_ord_date = 1992-01-01
      max_ord_date = 1998-08-02
#}

{% set min_ord_date = '1992-01-01' %}
{% set max_ord_date = '1998-08-02' %}

SELECT *
FROM {{ ref('stg_tpch_sf1__orders') }}
WHERE order_date < DATE '{{ min_ord_date }}'
   OR order_date > DATE '{{ max_ord_date }}'
