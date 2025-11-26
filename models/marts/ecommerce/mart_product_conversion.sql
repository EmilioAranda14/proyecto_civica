{{ config(
    materialized = 'table'
) }}

-- 1) Eventos relevantes del funnel (page_view y add_to_cart)
with events_base as (

    select
        fe.product_sk,
        fe.user_sk,
        fe.event_type
    from {{ ref('fact_events') }} as fe
    where fe.product_sk is not null
      and fe.event_type in ('page_view', 'add_to_cart')

),

-- 2) Vistas por producto
views_per_product as (

    select
        product_sk,
        count(*) as page_view_events
    from events_base
    where event_type = 'page_view'
    group by product_sk

),

-- 3) Add-to-cart por producto
add_to_cart_per_product as (

    select
        product_sk,
        count(*)              as add_to_cart_events,
        count(distinct user_sk) as add_to_cart_users
    from events_base
    where event_type = 'add_to_cart'
    group by product_sk

),

-- 4) Compras por producto (desde fact_order_items)
purchases_per_product as (

    select
        foi.product_sk,
        count(distinct foi.order_id) as purchase_orders,
        sum(foi.quantity)            as purchase_quantity,
        sum(foi.order_total_line)    as purchase_revenue
    from {{ ref('fact_order_items') }} as foi
    group by foi.product_sk

),

-- 5) Universo de productos que aparecen en eventos o en pedidos
product_universe as (

    select product_sk from views_per_product
    union
    select product_sk from add_to_cart_per_product
    union
    select product_sk from purchases_per_product

),

-- 6) Join con la dimensión de producto y métricas agregadas
joined as (

    select
        pu.product_sk,
        dp.product_id,
        dp.product_name,

        coalesce(v.page_view_events,      0) as page_view_events,
        coalesce(a.add_to_cart_events,    0) as add_to_cart_events,
        coalesce(a.add_to_cart_users,     0) as add_to_cart_users,
        coalesce(p.purchase_orders,       0) as purchase_orders,
        coalesce(p.purchase_quantity,     0) as purchase_quantity,
        coalesce(p.purchase_revenue,      0) as purchase_revenue
    from product_universe pu
    left join {{ ref('dim_products') }}      as dp on pu.product_sk = dp.product_sk
    left join views_per_product              as v  on pu.product_sk = v.product_sk
    left join add_to_cart_per_product        as a  on pu.product_sk = a.product_sk
    left join purchases_per_product          as p  on pu.product_sk = p.product_sk

),

-- 7) Cálculo de tasas de conversión y rankings
final as (

    select
        product_sk,
        product_id,
        product_name,

        page_view_events,
        add_to_cart_events,
        add_to_cart_users,
        purchase_orders,
        purchase_quantity,
        purchase_revenue,

        case
            when page_view_events = 0 then null
            else add_to_cart_events::float / page_view_events
        end as view_to_add_to_cart_rate,

        case
            when add_to_cart_events = 0 then null
            else purchase_orders::float / add_to_cart_events
        end as add_to_cart_to_purchase_rate,

        row_number() over (order by purchase_quantity desc)    as rank_by_quantity,
        row_number() over (order by purchase_revenue  desc)    as rank_by_revenue,
        row_number() over (order by view_to_add_to_cart_rate desc nulls last)
                                                               as rank_by_view_to_cart_rate
    from joined

)

select *
from final
