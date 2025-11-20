{{ config(materialized='view') }}

with base as (
    select distinct
        coalesce(
            nullif(lower(trim(shipping_service)), ''),
            'unknown'
        ) as shipping_service_code
    from {{ source('proyecto_civica_dev_bronze', 'raw_orders') }}
),
extra_unknown as (
    -- añadimos la fila 'unknown' en los valores que tengan shipping_service vacío
    select 'unknown' as shipping_service_code
),

unioned as (
    select shipping_service_code
    from base
    union all
    select shipping_service_code
    from extra_unknown
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['shipping_service_code']) }} as shipping_service_sk,
        shipping_service_code                                             as desc_shipping_service
    from unioned
)
 
select *
from final