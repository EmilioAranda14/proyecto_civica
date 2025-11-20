{{ config(
    materialized = 'view'
) }}

with src_addresses as (

    select *
    from {{ source('proyecto_civica_dev_bronze', 'raw_addresses') }}

),

addresses_change as (

    select
        address_id::varchar                                      as address_id,
        cast(zipcode as varchar)                                 as zipcode,
        ltrim(address, ' 0123456789-')                           as address_line,
        country::varchar                                         as country, 
        state::varchar                                           as state,
        convert_timezone('UTC', _fivetran_synced)                as date_load
    from src_addresses

)

select *
from addresses_change
