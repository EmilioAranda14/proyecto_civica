{{ config(
    materialized = 'view'
) }}

with src_addresses as (

    select *
    from {{ source('proyecto_civica_dev_bronze', 'raw_addresses') }}

),

addresses_change as (

    select
        address_id::varchar                       as address_id,
        cast(zipcode as varchar)                  as zipcode,
        ltrim(address, ' 0123456789-')            as address_line,
        country::varchar                          as country,
        state::varchar                            as state,
        {{ dbt_utils.generate_surrogate_key(['country']) }}                     as country_sk,
        {{ dbt_utils.generate_surrogate_key(['country', 'state']) }}            as state_sk,
        {{ dbt_utils.generate_surrogate_key(['country', 'state', 'zipcode']) }} as zipcode_sk,

        convert_timezone('UTC', _fivetran_synced) as date_load
    from src_addresses

)

select *
from addresses_change
