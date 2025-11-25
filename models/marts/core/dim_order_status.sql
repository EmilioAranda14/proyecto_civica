{{ config(
    materialized = 'table'
) }}

with source as (

    select
        order_status_sk,
        order_status_code,
        order_status_desc
    from {{ ref('stg_proyecto_civica__order_status') }}

),

final as (

    select
        order_status_sk,
        order_status_code,
        order_status_desc
    from source

)

select *
from final


