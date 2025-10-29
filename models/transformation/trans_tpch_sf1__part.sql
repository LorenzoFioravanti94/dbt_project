WITH
stage_part AS(
    SELECT *
    FROM {{ ref('stg_tpch_sf1__part') }}
),
final_part AS(
    SELECT part_id, manufacturer::INTEGER AS manufacturer, brand::INTEGER AS brand, UPPER(TRIM(colour_description)) AS colour_description,
        UPPER(TRIM(material_description)) AS material_description, container, retail_price, size_cm
    FROM stage_part
)
SELECT *
FROM final_part