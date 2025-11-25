{{ config(materialized = 'view') }}

with base as (

    select distinct
        coalesce(
            nullif(lower(trim(status)), ''),
            'unknown'
        ) as order_status_code
    from {{ ref('stg_proyecto_civica__orders') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['order_status_code']) }} as order_status_sk,
        order_status_code,
        order_status_code as order_status_desc
    from base

)

select *
from final
