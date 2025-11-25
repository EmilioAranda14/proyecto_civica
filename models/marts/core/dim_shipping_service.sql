{{ config(
    materialized = 'table'
) }}

with source as (

    select
        shipping_service_sk,
        shipping_service_id,
        shipping_service_name
    from {{ ref('stg_proyecto_civica__shipping_service') }}

),

final as (

    select
        shipping_service_sk,
        shipping_service_id,
        shipping_service_name
    from source

)

select *
from final

