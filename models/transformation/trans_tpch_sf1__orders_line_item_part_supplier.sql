WITH
stage_orders AS(
    SELECT *
    FROM {{ ref('stg_tpch_sf1__orders') }}
),
stage_line_item AS(
    SELECT *
    FROM {{ ref('stg_tpch_sf1__line_item') }}
),
stage_part_supplier AS(
    SELECT *
    FROM {{ ref('stg_tpch_sf1__part_supplier') }}
),

trans_orders AS(
    SELECT order_id, customer_id, UPPER(TRIM(status)) AS status, COALESCE(price, 0) AS price, order_date,
        SUBSTRING(order_priority, 1, 1)::INTEGER AS order_priority, clerk
    FROM stage_orders
),
trans_line_item AS(
    SELECT order_id, part_id, supplier_id, line_number, quantity::INTEGER AS quantity, COALESCE(extended_price, 0) AS extended_price,
        discount_percentage::INTEGER AS discount_percentage, tax_percentage::INTEGER AS tax_percentage,
        UPPER(TRIM(return_flag)) AS return_flag, UPPER(TRIM(line_status)) AS line_status, ship_date, commit_date,
        receipt_date, UPPER(TRIM(ship_instructions)) AS ship_instructions, UPPER(TRIM(ship_mode)) AS ship_mode
    FROM stage_line_item
),
trans_part_supplier AS(
    SELECT part_id, supplier_id, available_quantity::INTEGER AS available_quantity, supply_cost
    FROM stage_part_supplier
    WHERE supply_cost > 0
),
final_orders_line_item_part_supplier AS(
    SELECT tor.order_id, tor.customer_id, li.part_id, li.supplier_id, tor.order_date, li.ship_date, li.commit_date, li.receipt_date,
        tor.status AS order_status, li.line_status, li.return_flag, tor.order_priority, li.line_number, tor.clerk,
        li.quantity AS sold_quantity, ps.available_quantity AS stock_quantity, tor.price AS order_total_price, li.extended_price AS line_price,
        ps.supply_cost, li.discount_percentage AS line_discount_percentage, li.tax_percentage AS line_tax_percentage, li.ship_mode,
        li.ship_instructions
    FROM trans_orders AS tor
    INNER JOIN trans_line_item AS li
        ON tor.order_id = li.order_id
    INNER JOIN trans_part_supplier AS ps
        ON li.part_id = ps.part_id AND li.supplier_id = ps.supplier_id
)
SELECT *
FROM final_orders_line_item_part_supplier

