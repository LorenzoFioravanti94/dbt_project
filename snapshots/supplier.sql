{% 's_name' column not saved beacuse static, 's_comment' column not saved because useless}

{% snapshot part_supplier %}

    {{
      config(
        target_database ='LORENZO_DB',
        target_schema='snapshots',
        unique_key='s_suppkey',
        strategy='check',
        check_cols=['s_address', 's_nationkey', 's_phone', 's_acctbal']
      )
    }}

    SELECT *
    FROM {{ source("tpch_sf1", "supplier") }}

{% endsnapshot %}