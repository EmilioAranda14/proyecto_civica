{% snapshot product_price_s_timestamp %}

{{
  config(
    target_schema = 'snapshots',        
    target_database = 'PROYECTO_CIVICA_DEV_SILVER',   
    unique_key    = 'product_id',       
    strategy      = 'timestamp',
    updated_at    = '_fivetran_synced'  
  )
}}

select
    *
from {{ source('proyecto_civica_dev_bronze', 'raw_products') }}

{% endsnapshot %}
