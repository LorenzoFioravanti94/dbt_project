WITH
base_part AS (
    SELECT *
    FROM {{ source("tpch_sf1", "part") }}
),
refined AS (
    SELECT
        p_partkey AS part_id,
        p_name AS colour_description,
        SUBSTRING(p_mfgr, 14, 2) AS manufacturer,
        SUBSTRING(p_brand, 7, 3) AS brand,
        p_type AS material_description,
        p_size AS size_cm,
        p_container AS container,
        p_retailprice AS retail_price,
        p_comment AS comment
    FROM base_part
)
SELECT *
FROM refined