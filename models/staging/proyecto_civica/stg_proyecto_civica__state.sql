{{ config(materialized='view') }}

with base as (

    select distinct
        country,
        state
    from {{ source('proyecto_civica_dev_bronze', 'raw_addresses') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['country', 'state']) }} as state_sk, 
        state                                                        as desc_state,
        {{ dbt_utils.generate_surrogate_key(['country']) }}          as country_sk
    from base

)

select *
from final
