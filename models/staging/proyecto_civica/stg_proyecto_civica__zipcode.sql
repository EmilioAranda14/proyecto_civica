{{ config(materialized='view') }}

with base as (

    select distinct
        cast(zipcode as varchar) as zipcode,
        country,
        state
    from {{ source('proyecto_civica_dev_bronze', 'raw_addresses') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['country', 'state', 'zipcode']) }} as zipcode_sk,
        zipcode                                                                 as desc_zipcode,
        {{ dbt_utils.generate_surrogate_key(['country', 'state']) }}            as state_sk,
        {{ dbt_utils.generate_surrogate_key(['country']) }}                     as country_sk
    from base

)

select *
from final
