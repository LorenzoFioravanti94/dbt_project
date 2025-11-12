{# 
  Since the source database contains sales records from the '90s, 
  every NEW line item entry must reference a row within that period
  for all date columns in the table.

  Date limits were computed with:

    SELECT MIN(ship_date) AS min_ship_date, MAX(ship_date) AS max_ship_date,
        MIN(commit_date) AS min_commit_date, MAX(commit_date) AS max_commit_date,
        MIN(receipt_date) AS min_receipt_date, MAX(receipt_date) AS max_receipt_date
    FROM stg_tpch_sf1__line_item;

      min_ship_date = 1992-01-02
      max_ship_date = 1998-12-01
      min_commit_date = 1992-01-31
      max_commit_date = 1998-10-31
      min_receipt_date = 1992-01-04
      max_receipt_date = 1998-12-31
#}

{% set min_ship_date = '1992-01-02' %}
{% set max_ship_date = '1998-12-01' %}
{% set min_commit_date = '1992-01-31' %}
{% set max_commit_date = '1998-10-31' %}
{% set min_receipt_date = '1992-01-04' %}
{% set max_receipt_date = '1998-12-31' %}

SELECT *
FROM {{ ref('stg_tpch_sf1__line_item') }}
WHERE (ship_date < DATE '{{ min_ship_date }}' OR ship_date > DATE '{{ max_ship_date }}')
    OR
    (commit_date < DATE '{{ min_commit_date }}' OR commit_date > DATE '{{ max_commit_date }}')
    OR
    (receipt_date < DATE '{{ min_receipt_date }}' OR receipt_date > DATE '{{ max_receipt_date }}')