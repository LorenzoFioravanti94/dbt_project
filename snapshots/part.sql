{% 'p_comment' column not saved because useless}

{% snapshot part_snapshot %}

    {{
      config(
        target_database ='LORENZO_DB',
        target_schema='snapshots',
        unique_key='p_partkey',
        strategy='check',
        check_cols=['p_name', 'p_mfgr', 'p_brand', 'p_type', 'p_size', 'p_container', 'p_retailprice']
      )
    }}

    SELECT *
    FROM {{ source("tpch_sf1", "part") }}

{% endsnapshot %}