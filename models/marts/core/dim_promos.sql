{{ config(
    materialized = 'table'
) }}

with source as (

    select
        promo_sk,
        promo_id,
        promo_desc,
        promo_value,
        promo_status,
        promo_status_sk
    from {{ ref('stg_proyecto_civica__promos') }}

),

final as (

    select
        promo_sk,
        promo_id,
        promo_desc,
        promo_value,
        promo_status,
        promo_status_sk
    from source

)

select *
from final
