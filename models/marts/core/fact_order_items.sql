{{ config(
    materialized      = 'incremental',
    unique_key        = 'order_items_id',
    on_schema_change  = 'sync_all_columns'
) }}

-- 1) Líneas de pedido con shipping y promo a nivel de línea
WITH order_items_base AS (

    SELECT
        oi.order_items_id,
        oi.order_id,
        oi.product_id,
        oi.quantity,
        oi.date_load,

        oi.order_total_quantity,
        oi.shipping_cost_order,
        oi.shipping_cost_line,
        oi.promo_amount_line
    FROM {{ ref('int_proyecto_civica__order_items_promos') }} oi

    {% if is_incremental() %}
      WHERE oi.date_load > (
        SELECT max(date_load)
        FROM {{ this }}
      )
    {% endif %}

),

-- 2) Cabecera del pedido desde el staging
orders AS (

    SELECT
        o.order_id,
        o.user_id,
        o.promo_id,
        o.status,
        o.address_id,
        o.shipping_service_id,

        o.order_cost,
        o.order_total,

        o.tracking_id,
        o.order_created_at,
        o.estimated_delivery_at,
        o.delivered_at
    FROM {{ ref('stg_proyecto_civica__orders') }} o

),

-- 3) Unimos cabecera + línea y calculamos order_cost_line y order_total_line
order_items_joined AS (

    SELECT
        oi.order_items_id,
        oi.order_id,
        oi.product_id,
        oi.quantity,
        oi.order_total_quantity,
        oi.shipping_cost_order,
        oi.shipping_cost_line,
        oi.promo_amount_line,
        oi.date_load,

        o.user_id,
        o.promo_id,
        o.status,
        o.address_id,
        o.shipping_service_id,

        -- métricas de pedido
        o.order_cost,
        o.order_total,

        -- reparto del order_cost a nivel de línea
        CASE
            WHEN oi.order_total_quantity IS NULL
              OR oi.order_total_quantity = 0
              OR o.order_cost IS NULL
            THEN NULL
            ELSE (o.order_cost / oi.order_total_quantity) * oi.quantity
        END AS order_cost_line,

        -- reparto del order_total a nivel de línea
        CASE
            WHEN oi.order_total_quantity IS NULL
              OR oi.order_total_quantity = 0
              OR o.order_total IS NULL
            THEN NULL
            ELSE (o.order_total / oi.order_total_quantity) * oi.quantity
        END AS order_total_line,

        o.tracking_id,
        o.order_created_at,
        o.estimated_delivery_at,
        o.delivered_at
    FROM order_items_base oi
    LEFT JOIN orders o
      ON oi.order_id = o.order_id

),

-- 4) Enriquecemos con dimensiones (users, products, promos, order_status, address, shipping_service, date)
order_items_enriched AS (

    SELECT
        -- PK del fact
        oij.order_items_id,

        -- FKs a dimensiones
        du.user_sk,
        dp.product_sk,
        dpr.promo_sk,
        dos.order_status_sk,
        da.address_sk,
        dss.shipping_service_sk,
        dd.date_day                         AS order_date,

        -- claves naturales
        oij.order_id, 

        -- métricas a nivel línea
        oij.quantity,
        oij.order_total_quantity,
        oij.shipping_cost_order,
        oij.shipping_cost_line,
        oij.promo_amount_line,
        -- 🔽 forzamos mismo tipo NUMBER(16,4) que ya existe en la tabla
        oij.order_cost_line::NUMBER(16,4)   AS order_cost_line,
        oij.order_total_line::NUMBER(16,4)  AS order_total_line,

        -- fechas y tracking
        oij.order_created_at,
        oij.estimated_delivery_at,
        oij.delivered_at,
        oij.tracking_id,

        oij.date_load

    FROM order_items_joined oij

    -- dim_users
    LEFT JOIN {{ ref('dim_users') }} du
        ON oij.user_id = du.user_id

    -- dim_products
    LEFT JOIN {{ ref('dim_products') }} dp
        ON oij.product_id = dp.product_id

    -- dim_promos
    LEFT JOIN {{ ref('dim_promos') }} dpr
        ON oij.promo_id = dpr.promo_id

    -- dim_order_status
    LEFT JOIN {{ ref('dim_order_status') }} dos
        ON oij.status = dos.order_status_code

    -- dim_addresses
    LEFT JOIN {{ ref('dim_addresses') }} da
        ON oij.address_id = da.address_id

   -- dim_shipping_service
    LEFT JOIN {{ ref('dim_shipping_service') }} dss
    ON oij.shipping_service_id = dss.shipping_service_sk
        

    -- dim_date (fecha del pedido)
    LEFT JOIN {{ ref('dim_date') }} dd
        ON oij.order_created_at::date = dd.date_day
),

final AS (
    SELECT *
    FROM order_items_enriched
)

SELECT *
FROM final


