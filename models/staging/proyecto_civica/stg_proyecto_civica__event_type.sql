{{ config(materialized = 'view') }}

with base as (

    select distinct
        coalesce(
            nullif(lower(trim(event_type)), ''),
            'unknown'
        ) as event_type_code
    from {{ ref('stg_proyecto_civica__events') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['event_type_code']) }} as event_type_sk,
        event_type_code
    from base

)

select *
from final
