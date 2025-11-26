{{ config(
    materialized = 'table'
) }}

with source as (

    select
        address_id,
        address_line,
        zipcode,
        state,
        country
    from {{ ref('stg_proyecto_civica__addresses') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['address_id']) }} as address_sk,
        address_id,
        address_line,
        zipcode,
        state,
        country
    from source

)

select * from final

