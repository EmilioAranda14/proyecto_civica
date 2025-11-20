{{ config(materialized='view') }}

with base as (

    select distinct
        status
    from {{ ref('stg_proyecto_civica__orders') }}
    where status is not null

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['status']) }} as order_status_sk,
        status                                             as order_status_code
    from base

)

select *
from final