{% snapshot promos_discount_s_check %}

{{
    config(
        target_schema = 'SNAPSHOTS',
        strategy      = 'check',
        unique_key    = 'promo_id',
        check_cols    = ['promo_desc', 'promo_value', 'promo_status']
    )
}}

select
    promo_desc,
    promo_value,
    promo_status,
    date_load
from {{ ref('stg_proyecto_civica__promos') }}

{% endsnapshot %}

