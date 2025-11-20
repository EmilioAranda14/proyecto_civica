{{ config(materialized='view') }}

with base as (

    select distinct
        promo_status
    from {{ ref('stg_proyecto_civica__promos') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['promo_status']) }} as promo_status_sk,
        promo_status                                             as promo_status_code
    from base

)

select *
from final