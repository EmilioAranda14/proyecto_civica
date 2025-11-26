{{ config(
    materialized = 'table'
) }}

-- 1) Subimos de línea a pedido por shipping_service
with orders_by_shipping as (

    select
        foi.shipping_service_sk,
        foi.order_id,

        -- tiempos del pedido (mismas fechas para todas las líneas del pedido)
        min(foi.order_created_at)      as order_created_at,
        max(foi.estimated_delivery_at) as estimated_delivery_at,
        max(foi.delivered_at)          as delivered_at,

        -- coste de envío del pedido (mismo valor en todas las líneas)
        max(foi.shipping_cost_order)   as shipping_cost_total,

        -- total de pedido sumando las líneas
        sum(foi.order_total_line)      as order_total
    from {{ ref('fact_order_items') }} as foi
    group by
        foi.shipping_service_sk,
        foi.order_id
),

-- 2) Métricas de pedido (días, on_time, % shipping)
orders_enriched as (

    select
        shipping_service_sk,
        order_id,
        order_total,
        shipping_cost_total,
        order_created_at,
        estimated_delivery_at,
        delivered_at,

        case
            when delivered_at is null
                 or order_created_at is null
            then null
            else datediff('day', order_created_at, delivered_at)
        end as days_to_deliver,

        case
            when delivered_at is null
                 or estimated_delivery_at is null
            then null
            when delivered_at <= estimated_delivery_at then 1
            else 0
        end as delivered_on_time_flag,

        case
            when order_total is null or order_total = 0 then null
            else shipping_cost_total / order_total
        end as shipping_cost_pct
    from orders_by_shipping
),

-- 3) Agregamos por shipping_service con la dimensión
agg as (

    select
        dss.shipping_service_sk,
        dss.shipping_service_id,
        dss.shipping_service_name,

        count(*)                          as orders_count,
        sum(oe.order_total)              as revenue_total,
        sum(oe.shipping_cost_total)      as shipping_cost_total,

        avg(oe.shipping_cost_pct)        as avg_shipping_cost_pct,
        avg(oe.days_to_deliver)          as avg_days_to_deliver,

        avg(oe.delivered_on_time_flag)   as on_time_rate
    from orders_enriched oe
    left join {{ ref('dim_shipping_service') }} dss
        on oe.shipping_service_sk = dss.shipping_service_sk
    group by
        dss.shipping_service_sk,
        dss.shipping_service_id,
        dss.shipping_service_name
),

profile as (

    select
        *,
        row_number() over (order by orders_count desc) as rank_by_orders
    from agg
)

select *
from profile
