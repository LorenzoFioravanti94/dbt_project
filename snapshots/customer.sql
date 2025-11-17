{% 'c_name' column not saved beacuse static, 'c_comment' column not saved because useless}

{% snapshot customer_snapshot %}

    {{
      config(
        target_database ='LORENZO_DB',
        target_schema='snapshots',
        unique_key='c_custkey',
        strategy='check',
        check_cols=['c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment']
      )
    }}

    SELECT *
    FROM {{ source("tpch_sf1", "customer") }}

{% endsnapshot %}
