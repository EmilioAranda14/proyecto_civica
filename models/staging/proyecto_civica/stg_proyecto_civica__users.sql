{{ config(
    materialized = 'view'
) }}

with src_users as (

    select *
    from {{ source('proyecto_civica_dev_bronze', 'raw_users') }}

),

filtered as (

    select *
    from src_users
    where _fivetran_deleted is null

),

users_change as (

    select
        user_id::varchar          as user_id,
        first_name::varchar       as first_name,
        last_name::varchar        as last_name,
        email::varchar            as email,
        phone_number::varchar     as phone_number,
        address_id::varchar       as address_id,
        total_orders::number(12,0) as total_orders,

        convert_timezone('UTC', created_at)       as user_created_at,
        convert_timezone('UTC', updated_at)       as user_updated_at,
        convert_timezone('UTC', _fivetran_synced) as date_load,

        {{ dbt_utils.generate_surrogate_key(['user_id']) }} as user_sk

    from filtered

)

select *
from users_change
