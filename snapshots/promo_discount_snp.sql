{% snapshot promos_discount_s_check %}

{{
  config(
    target_schema = 'snapshots',
    unique_key    = 'promo_id',
    strategy      = 'check',
    check_cols    = ['discount', 'promo_status', 'desc_promo']
  )
}}

select
    promo_id,
    desc_promo,
    discount,
    promo_status,
    date_load
from {{ ref('stg_proyecto_civica__promos') }}

{% endsnapshot %}

