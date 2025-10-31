{{
    config(
        materialized = 'table'
    )
}}

WITH
trans_orders_line_item_part_supplier AS(
    SELECT *
    FROM {{ ref('trans_tpch_sf1__orders_line_item_part_supplier') }}
),
dim_date AS(
    SELECT *
    FROM {{ ref('dim_date') }}
),

hash_code_for_dates AS(
    SELECT t.order_id, t.part_id, t.supplier_id, t.line_number, t.customer_id,
        dd_order.date_id AS order_date_id, dd_ship.date_id AS ship_date_id, dd_commit.date_id AS commit_date_id,
        dd_receipt.date_id AS receipt_date_id,
        t.order_status, t.line_status, t.return_flag, t.order_priority, t.order_clerk, t.sold_quantity, t.stock_quantity,
        t.discount_percentage, t.tax_percentage, t.order_gross_price, t.order_net_price, t.line_gross_price,
        t.line_discount_value AS line_discount_amount, t.line_net_price, t.supply_cost, t.ship_mode, t.ship_instructions
    FROM trans_orders_line_item_part_supplier AS t
    LEFT JOIN dim_date AS dd_order
        ON t.order_date = dd_order.date_value
    LEFT JOIN dim_date AS dd_ship
        ON t.ship_date = dd_ship.date_value
    LEFT JOIN dim_date AS dd_commit
        ON t.commit_date = dd_commit.date_value
    LEFT JOIN dim_date AS dd_receipt
        ON t.receipt_date = dd_receipt.date_value
),
sale_line_surrogate_key AS(
    SELECT {{ dbt_utils.generate_surrogate_key(['order_id', 'part_id', 'supplier_id', 'line_number']) }} AS sale_line_id,
        order_id, part_id, supplier_id, line_number, customer_id, order_date_id, ship_date_id, commit_date_id,
        receipt_date_id, order_status, line_status, return_flag, order_priority, order_clerk, sold_quantity, stock_quantity,
        discount_percentage, tax_percentage, order_gross_price, order_net_price, line_gross_price, line_discount_amount, line_net_price,
        supply_cost, ship_mode, ship_instructions
    FROM hash_code_for_dates
),
measures AS(
    SELECT sale_line_id,
        ROUND((line_gross_price * (1 - discount_percentage / 100.0) * (tax_percentage / 100.0)), 2) AS line_tax_amount,
        ROUND((line_net_price - (supply_cost * sold_quantity)), 2) AS line_profit,
        CASE
            WHEN discount_percentage > 0 THEN TRUE
            ELSE FALSE
            END AS is_discounted
    FROM sale_line_surrogate_key
),
fact_sale_line AS(
    SELECT s.sale_line_id, s.order_id, s.part_id, s.supplier_id, s.line_number, s.customer_id, s.order_date_id, s.ship_date_id,
        s.commit_date_id, s.receipt_date_id, s.order_status, s.line_status, s.return_flag, s.order_priority, s.order_clerk, 
        s.stock_quantity, s.discount_percentage, s.tax_percentage, s.order_gross_price, s.order_net_price, s.line_gross_price,
        s.line_discount_amount, m.line_tax_amount, s.line_net_price, s.supply_cost, s.sold_quantity, m.line_profit, m.is_discounted,
        s.ship_mode, s.ship_instructions
    FROM sale_line_surrogate_key AS s
    INNER JOIN measures AS m
        ON s.sale_line_id = m.sale_line_id
)
SELECT *
FROM fact_sale_line