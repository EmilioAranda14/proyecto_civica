{{ config(
    materialized      = 'incremental',
    unique_key        = 'order_items_id',
    on_schema_change  = 'sync_all_columns'
) }}

-- 1) Líneas de pedido con shipping y promo a nivel de línea
with order_items_base as (

    select
        oi.order_items_id,
        oi.order_id,
        oi.product_id,
        oi.quantity,
        oi.date_load,

        oi.order_total_quantity,
        oi.shipping_cost_order,
        oi.shipping_cost_unit,
        oi.shipping_cost_line,

        oi.promo_amount_order,
        oi.promo_amount_unit,
        oi.promo_amount_line
    from {{ ref('int_proyecto_civica__order_items_promos') }} oi

    {% if is_incremental() %}
      where oi.date_load > (
        select max(date_load)
        from {{ this }}
      )
    {% endif %}

),

-- 2) Cabecera del pedido desde el staging
orders as (

    select
        o.order_id,
        o.user_id,
        o.promo_id,

        o.order_cost,
        o.order_total,

        o.tracking_id,
        o.order_created_at,
        o.estimated_delivery_at,
        o.delivered_at,

        o.status,
        o.address_id
    from {{ ref('stg_proyecto_civica__orders') }} o

),

-- 3) Unimos cabecera + línea al grano línea
order_items_joined as (

    select
        oi.order_items_id,
        oi.order_id,
        oi.product_id,
        oi.quantity,
        oi.order_total_quantity,
        oi.shipping_cost_order,
        oi.shipping_cost_unit,
        oi.shipping_cost_line,
        oi.promo_amount_order,
        oi.promo_amount_unit,
        oi.promo_amount_line,
        oi.date_load,

        o.user_id,
        o.promo_id,
        o.order_cost,
        o.order_total,
        o.tracking_id,
        o.order_created_at,
        o.estimated_delivery_at,
        o.delivered_at,
        o.status,
        o.address_id
    from order_items_base oi
    left join orders o
      on oi.order_id = o.order_id

),

-- 4) Enriquecemos con dimensiones (users, products, promos, order_status, address, date)
order_items_enriched as (

    select
        oij.order_items_id,

        -- claves naturales
        oij.order_id,
        oij.product_id,
        oij.user_id,
        oij.promo_id,
        oij.status,
        oij.address_id,

        -- FKs a dimensiones
        du.user_sk,
        dp.product_sk,
        dpr.promo_sk,
        dos.order_status_sk,
        da.address_sk,
        dd.date_day as order_date,

        -- métricas a nivel línea
        oij.quantity,
        oij.order_total_quantity,
        oij.shipping_cost_order,
        oij.shipping_cost_unit,
        oij.shipping_cost_line,
        oij.promo_amount_order,
        oij.promo_amount_unit,
        oij.promo_amount_line,

        -- métricas de pedido (repetidas por línea)
        oij.order_cost,
        oij.order_total,

        -- fechas y tracking
        oij.order_created_at,
        oij.estimated_delivery_at,
        oij.delivered_at,
        oij.tracking_id,

        oij.date_load

    from order_items_joined oij

    -- dim_users
    left join {{ ref('dim_users') }} du
        on oij.user_id = du.user_id

    -- dim_products
    left join {{ ref('dim_products') }} dp
        on oij.product_id = dp.product_id

    -- dim_promos
    left join {{ ref('dim_promos') }} dpr
        on oij.promo_id = dpr.promo_id

    -- dim_order_status (status viene tal cual de stg_orders)
    left join {{ ref('dim_order_status') }} dos
        on oij.status = dos.order_status_code

    -- dim_addresses
    left join {{ ref('dim_addresses') }} da
        on oij.address_id = da.address_id

    -- dim_date
    left join {{ ref('dim_date') }} dd
        on oij.order_created_at::date = dd.date_day
),

final as (

    select *
    from order_items_enriched

)

select *
from final
