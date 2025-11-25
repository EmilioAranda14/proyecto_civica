{{ config(
    materialized = 'table'
) }}

with source as (

    select
        product_sk,
        product_id,
        product_name,
        product_price,
        is_in_stock
    from {{ ref('stg_proyecto_civica__products') }}

),

final as (

    select
        product_sk,
        product_id,
        product_name,
        product_price,
        is_in_stock
    from source

)

select *
from final
