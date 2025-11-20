{{ config(materialized='view') }}

with base as (
    select *
    from {{ ref('stg_proyecto_civica__promos') }}
),

joined as (
    select
        p.promo_id,
        p.discount,
        p.promo_status,
        ps.promo_status_sk
    from base p
    left join {{ ref('stg_proyecto_civica__promo_status') }} ps
        on p.promo_status = ps.promo_status_code
)

select *
from joined
