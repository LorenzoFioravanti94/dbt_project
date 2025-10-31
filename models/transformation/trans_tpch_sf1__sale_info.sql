WITH
stage_orders AS(
    SELECT *
    FROM {{ ref('stg_tpch_sf1__orders') }}
),
stage_line_item AS (
    SELECT *
    FROM {{ ref('stg_tpch_sf1__line_item') }}
),
stage_part_supplier AS (
    SELECT *
    FROM {{ ref('stg_tpch_sf1__part_supplier') }}
),

trans_orders AS( -- price = order_net_price
    SELECT order_id, customer_id, UPPER(TRIM(status)) AS order_status, COALESCE(price, 0) AS order_net_price, order_date,
        SUBSTRING(order_priority, 1, 1)::INTEGER AS order_priority, clerk AS order_clerk
    FROM stage_orders
),
trans_line_item AS(
    SELECT order_id, part_id, supplier_id, line_number, quantity::INTEGER AS sold_quantity, discount_percentage::INTEGER AS discount_percentage,
        tax_percentage::INTEGER AS tax_percentage, COALESCE(extended_price, 0) AS line_gross_price,
        UPPER(TRIM(return_flag)) AS return_flag, UPPER(TRIM(line_status)) AS line_status, ship_date, commit_date,
        receipt_date, UPPER(TRIM(ship_instructions)) AS ship_instructions, UPPER(TRIM(ship_mode)) AS ship_mode
    FROM stage_line_item
),
trans2_line_item AS(
    SELECT order_id, part_id, supplier_id, line_number, sold_quantity, discount_percentage, tax_percentage, line_gross_price,
        ROUND((line_gross_price * (discount_percentage / 100.0)), 2) AS line_discount_value,
        ROUND((line_gross_price * (1 - discount_percentage / 100.0) * (1 + tax_percentage / 100.0)), 2) AS line_net_price,
        return_flag, line_status, ship_date,
        commit_date, receipt_date, ship_instructions, ship_mode 
    FROM trans_line_item
),                   
gross_price_per_order AS (
    SELECT order_id, SUM(line_gross_price) AS order_gross_price
    FROM trans2_line_item
    GROUP BY order_id
),
trans_part_supplier AS(
    SELECT part_id, supplier_id, available_quantity::INTEGER AS stock_quantity, supply_cost
    FROM stage_part_supplier
    WHERE supply_cost > 0
),


final_line_item_per_order_part_supplier AS (
    SELECT tor.order_id, tor.customer_id,li.part_id, li.supplier_id, tor.order_date, li.ship_date, li.commit_date, li.receipt_date,
        tor.order_status, li.line_status, li.return_flag, tor.order_priority, li.line_number, tor.order_clerk, li.sold_quantity,
        ps.stock_quantity, li.discount_percentage, li.tax_percentage, gppo.order_gross_price, tor.order_net_price, li.line_gross_price,
        li.line_discount_value, li.line_net_price, ps.supply_cost, li.ship_mode, li.ship_instructions
    FROM trans_orders AS tor
    INNER JOIN trans2_line_item AS li
        ON tor.order_id = li.order_id
    INNER JOIN trans_part_supplier AS ps
        ON li.part_id = ps.part_id AND li.supplier_id = ps.supplier_id
    INNER JOIN gross_price_per_order AS gppo
        ON tor.order_id = gppo.order_id
)
SELECT *
FROM final_line_item_per_order_part_supplier
ORDER BY order_id ASC