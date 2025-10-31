WITH
trans_country_per_region AS(
    SELECT *
    FROM {{ ref('trans_tpch_sf1__country_per_region') }}
),
dim_geography AS(
    SELECT country_id AS geography_id, country_name, region_name
    FROM trans_country_per_region
)
SELECT *
FROM dim_geography
ORDER BY geography_id ASC