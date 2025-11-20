{{ config(materialized='view') }}

with base as (
    select distinct
            country
    from {{ source('proyecto_civica_dev_bronze', 'raw_addresses') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['country']) }} as country_sk,
        country                                             as desc_country
    from base

)

select *
from final