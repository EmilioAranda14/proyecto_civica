{{ config(
    materialized = 'table'
) }}

with source as (

    select
        user_sk,
        user_id,
        user_created_at,
        first_name,
        last_name,
        email,
        phone_number,
        total_orders,
        address_id
    from {{ ref('stg_proyecto_civica__users') }}

),

final as (

    select
        user_sk,
        user_id,
        user_created_at,
        first_name,
        last_name,
        email,
        phone_number,
        address_id,
        first_name || ' ' || last_name as user_full_name
    from source

)

select *
from final

