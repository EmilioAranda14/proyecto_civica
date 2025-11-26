{{ config(
    materialized = 'incremental',
    unique_key   = 'order_items_id'
) }}

-- Todas las líneas de pedido desde el staging
WITH order_items_all AS (
    SELECT
        order_items_id,
        order_id,
        product_id,
        quantity,
        date_load
    FROM {{ ref('stg_proyecto_civica__order_items') }}
),

-- Pedidos + info de promo (importe de descuento del pedido)
orders_with_promo AS (
    SELECT
        o.order_id,
        o.shipping_cost,
        o.order_cost,
        o.order_total,
        o.promo_id,
        o.date_load,
        COALESCE(p.promo_value, 0) AS promo_amount_order   -- importe de promo a nivel pedido
    FROM {{ ref('stg_proyecto_civica__orders') }} o
    LEFT JOIN {{ ref('stg_proyecto_civica__promos') }} p
        ON o.promo_id = p.promo_id
),

-- Total de unidades por pedido
quantity_per_order AS (
    SELECT
        order_id,
        SUM(quantity) AS total_quantity
    FROM order_items_all
    GROUP BY order_id
),

-- Costes y totales por unidad dentro de cada pedido
shipping_cost_per_unit AS (
    SELECT
        o.order_id,
        o.shipping_cost,
        o.order_cost,
        o.order_total,
        qpo.total_quantity,

        -- coste de envío por unidad
        CASE
            WHEN qpo.total_quantity IS NULL
              OR qpo.total_quantity = 0
              OR o.shipping_cost IS NULL
            THEN NULL
            ELSE o.shipping_cost / qpo.total_quantity
        END AS shipping_cost_unit,

        -- order_cost por unidad
        CASE
            WHEN qpo.total_quantity IS NULL
              OR qpo.total_quantity = 0
              OR o.order_cost IS NULL
            THEN NULL
            ELSE o.order_cost / qpo.total_quantity
        END AS order_cost_unit,

        -- order_total por unidad
        CASE
            WHEN qpo.total_quantity IS NULL
              OR qpo.total_quantity = 0
              OR o.order_total IS NULL
            THEN NULL
            ELSE o.order_total / qpo.total_quantity
        END AS order_total_unit
    FROM orders_with_promo o
    LEFT JOIN quantity_per_order qpo
        ON o.order_id = qpo.order_id
),

-- Importe de promo por unidad dentro de cada pedido
promo_per_unit AS (
    SELECT
        o.order_id,
        o.promo_amount_order,
        qpo.total_quantity,
        CASE
            WHEN qpo.total_quantity IS NULL
              OR qpo.total_quantity = 0
              OR o.promo_amount_order IS NULL
            THEN 0
            ELSE o.promo_amount_order / qpo.total_quantity
        END AS promo_amount_unit
    FROM orders_with_promo o
    LEFT JOIN quantity_per_order qpo
        ON o.order_id = qpo.order_id
),

-- Solo las líneas nuevas/actualizadas (incremental por date_load)
order_items_new AS (
    SELECT
        *
    FROM order_items_all
    {% if is_incremental() %}
      WHERE date_load > (SELECT MAX(date_load) FROM {{ this }})
    {% endif %}
),

-- Resultado final: shipping + promo + totales a nivel de línea
final AS (
    SELECT
        oi.order_items_id,
        oi.order_id,
        oi.product_id,
        oi.quantity,
        oi.date_load,

        -- SHIPPING
        scp.shipping_cost::NUMBER(12,2) AS shipping_cost_order,
        scp.total_quantity              AS order_total_quantity,
        scp.shipping_cost_unit          AS shipping_cost_unit,
        (oi.quantity * scp.shipping_cost_unit)::NUMBER(12,2)
                                        AS shipping_cost_line,

        -- ORDER_COST (coste del pedido repartido)
        scp.order_cost::NUMBER(12,2)    AS order_cost_order,
        scp.order_cost_unit             AS order_cost_unit,
        (oi.quantity * scp.order_cost_unit)::NUMBER(12,2)
                                        AS order_cost_line,

        -- ORDER_TOTAL (importe total del pedido repartido)
        scp.order_total::NUMBER(12,2)   AS order_total_order,
        scp.order_total_unit            AS order_total_unit,
        (oi.quantity * scp.order_total_unit)::NUMBER(12,2)
                                        AS order_total_line,

        -- PROMO
        COALESCE(ppu.promo_amount_order, 0)::NUMBER(12,2)
                                        AS promo_amount_order,
        COALESCE(ppu.promo_amount_unit, 0)
                                        AS promo_amount_unit,
        (oi.quantity * COALESCE(ppu.promo_amount_unit, 0))::NUMBER(12,2)
                                        AS promo_amount_line
    FROM order_items_new oi
    LEFT JOIN shipping_cost_per_unit scp
        ON oi.order_id = scp.order_id
    LEFT JOIN promo_per_unit ppu
        ON oi.order_id = ppu.order_id
)

SELECT *
FROM final

 