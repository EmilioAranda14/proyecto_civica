{{ config(materialized='view') }}

with base as (

    select *
    from {{ ref('stg_proyecto_civica__orders') }}

),

joined as (

    select
        o.order_id,
        o.user_id, 
        o.address_id,
        o.promo_id,
        o.shipping_cost,
        o.order_cost,
        o.order_total,
        o.tracking_id,
        o.order_created_at,
        o.estimated_delivery_at,
        o.delivered_at,
        ss.shipping_service_sk,
        os.order_status_sk,
        ae.zipcode_sk,
        ae.state_sk,
        ae.country_sk
    from base o
    left join {{ ref('stg_proyecto_civica__shipping_service') }} ss
        on o.shipping_service_code = ss.desc_shipping_service
    left join {{ ref('stg_proyecto_civica__order_status') }} os
        on o.status = os.order_status_code
    left join {{ ref('int_proyecto_civica__addresses_enriched') }} ae
        on o.address_id = ae.address_id

)

select *
from joined 
