{{ config(materialized = 'view') }}

with base as (

    select distinct
        coalesce(
            nullif(lower(trim(shipping_service)), ''),
            'unknown'
        ) as shipping_service_id
    from {{ source('proyecto_civica_dev_bronze', 'raw_orders') }}
    where _fivetran_deleted is null

),

extra_unknown as (

    -- garantizamos que siempre exista la fila 'unknown'
    select 'unknown' as shipping_service_id

),

unioned as (

    select shipping_service_id from base
    union
    select shipping_service_id from extra_unknown

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['shipping_service_id']) }} as shipping_service_sk,
        shipping_service_id,
        shipping_service_id as shipping_service_name
    from unioned

)
 
select *
from final
