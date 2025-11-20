{{ config(materialized='view') }}

with base as (

    select distinct
        event_type
    from {{ source('proyecto_civica_dev_bronze', 'raw_events') }}
    where event_type is not null

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['event_type']) }} as event_type_sk,
        event_type                                             as event_type_code
    from base

)

select *
from final
