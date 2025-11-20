{{ config(materialized='view') }}

with base as (

    select *
    from {{ ref('stg_proyecto_civica__events') }}

),

joined as (

    select
        e.event_id,
        e.page_url,
        e.user_id,
        e.product_id,
        e.session_id,
        e.event_created_at,
        e.event_date,
        e.order_id,
        et.event_type_sk
    from base e
    left join {{ ref('stg_proyecto_civica__event_type') }} et
        on e.event_type = et.event_type_code

)

select * 
from joined

